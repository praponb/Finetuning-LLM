with filtered as (
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        lower(status) as status_lower,
        lower(carrier) as carrier_lower,
        coalesce(carrier, 'NONE') as carrier_filled
    from shipments
    where carrier is not null
      and carrier is distinct from 'DHL'
),
ranked as (
    select *, row_number() over (partition by order_id, status order by shipped_date desc, id) as row_num
    from filtered
    qualify row_num = 1
),
with_windows as (
    select
        *,
        lead(cost) over (partition by carrier order by row_num, id) as next_cost,
        lag(cost) over (partition by carrier order by row_num, id) as prev_cost,
        row_number() over (partition by carrier order by row_num, id) as seq_no,
        sum(cost) over (partition by status order by delivered_date, id rows between unbounded preceding and current row) as cumulative_cost
    from ranked
),
enriched as (
    select
        *,
        avg(cumulative_cost) over (partition by carrier order by delivered_date, id rows between unbounded preceding and current row) as running_avg_cumulative_cost,
        min(cumulative_cost) over (partition by carrier order by delivered_date, id rows between unbounded preceding and current row) as running_min_cumulative_cost,
        max(cumulative_cost) over (partition by carrier order by delivered_date, id rows between unbounded preceding and current row) as running_max_cumulative_cost,
        count(cumulative_cost) over (partition by carrier order by delivered_date, id rows between unbounded preceding and current row) as running_count_cumulative_cost
    from with_windows
    where year(delivered_date) = 2023
)
select
    id,
    carrier,
    shipped_date,
    delivered_date,
    status,
    carrier_filled,
    row_num,
    next_cost,
    prev_cost,
    case when carrier rlike '^(Kerry|DHL)$' then 'GROUP_B' else 'OTHER_ZONE' end as carrier_zone
from enriched
order by status desc nulls last
