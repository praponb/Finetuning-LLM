with base as (
    select
        *,
        datediff(current_date(), shipped_date) as days_since_shipped_date,
        case when carrier in ('DHL', 'UPS') then 'Tier 1' else 'Other' end as carrier_group,
        coalesce(carrier, 'N/A') as resolved_carrier
    from shipments
    where order_id is not null
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by order_id order by shipped_date desc nulls last, id) as latest_rank
        from base
    ) t
    where latest_rank = 1
),
scored as (
    select
        *,
        sum(days_since_shipped_date) over (partition by carrier order by cost, id rows between unbounded preceding and current row) as cumulative_days_since_shipped_date,
        row_number() over (partition by status order by delivered_date, id) as row_seq
    from ranked
),
enriched as (
    select
        *,
        round(cost / sum(cost) over (partition by status), 4) as cost_share,
        lag(cost, 1) over (partition by carrier order by cumulative_days_since_shipped_date, id) as prev_cost,
        ntile(3) over (partition by order_id order by cumulative_days_since_shipped_date desc, id) as cumulative_days_since_shipped_date_bucket
    from scored
),
enriched_shipments as (
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        days_since_shipped_date,
        carrier_group,
        resolved_carrier,
        latest_rank,
        cumulative_days_since_shipped_date,
        row_seq,
        cost_share,
        prev_cost,
        cumulative_days_since_shipped_date_bucket,
        min(prev_cost) over (partition by order_id, status) as order_id_min_prev_cost,
        first_value(cumulative_days_since_shipped_date) ignore nulls over (partition by carrier order by cost_share, id rows between unbounded preceding and current row) as first_known_cumulative_days_since_shipped_date
    from enriched
)
select
    s.id,
    s.order_id,
    s.carrier,
    s.shipped_date,
    s.delivered_date,
    s.cost,
    s.status,
    s.days_since_shipped_date,
    s.carrier_group,
    case when status rlike '^(IN_TRANSIT|LOST)$' then 'GROUP_B' when status rlike '^(DELIVERED|RETURNED)$' then 'OTHER_ZONE' else 'EXTENDED' end as status_zone,
    (select sum(t2.cost) from shipments t2 where t2.order_id = s.order_id) as sum_shipment_cost
from enriched_shipments s
order by status desc nulls last
