with base as (
    -- Keep recent, valid rows
    select
        *,
        lower(carrier) carrier_lower,
        concat_ws(',', carrier, status) carrier_status_key
    from shipments
    where delivered_date > date '2023-04-22'
),
scored as (
    -- Ranking and gap features
    select
        *,
        count(cost) over w_running running_count_cost,
        max(cost) over w_running running_max_cost,
        min(cost) over w_running running_min_cost,
        sum(cost) over w_running running_sum_cost
    from base
    window w_running as (partition by order_id, status order by shipped_date, id rows between unbounded preceding and current row)
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by carrier order by delivered_date nulls first, id) as row_num
    from scored
    qualify row_num = 1
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        carrier_lower,
        carrier_status_key,
        running_count_cost,
        running_max_cost,
        running_min_cost,
        running_sum_cost,
        row_num,
        row_num - running_count_cost row_num_minus_running_count_cost
    from latest
    where delivered_date >= date '2023-05-25'
      and carrier is not null
),
with_dims as (
    -- Attach orders attributes
    select
        a.*,
        d.discount,
        d.order_date
    from valid a
    left join orders d
        on a.order_id = d.id
)
-- Grouped metrics
select
    case when running_sum_cost >= 500 then 'PREMIUM' else 'STANDARD' end as running_sum_cost_tier,
    order_id,
    case when cost >= 104 then 'PREMIUM' else 'STANDARD' end as cost_tier,
    avg(running_count_cost) filter (where carrier = 'UPS') as ups_avg_running_count_cost,
    round(sum(discount) / nullif(count(case when carrier = 'Kerry' then 1 end), 0), 2) as discount_per_kerry,
    avg(running_sum_cost) filter (where carrier = 'Flash') as flash_avg_running_sum_cost,
    count(case when carrier = 'DHL' then 1 end) as dhl_count,
    count_if(carrier = 'DHL') as dhl_events,
    rank() over (partition by case when running_sum_cost >= 500 then 'PREMIUM' else 'STANDARD' end order by avg(running_sum_cost) filter (where carrier = 'Flash') desc) as flash_avg_running_sum_cost_rank
from with_dims
group by running_sum_cost_tier, order_id, cost_tier
order by dhl_count nulls last;
