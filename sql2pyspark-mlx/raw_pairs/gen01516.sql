with filtered as (
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        substring(status, 1, 2) status_prefix
    from shipments
    where status not in ('RETURNED', 'DELIVERED')
      and delivered_date > date '2023-03-10'
),
with_windows as (
    select
        id,
        order_id,
        status_prefix,
        ntile(5) over (partition by carrier order by cost, id) cost_bucket,
        rank() over (partition by order_id order by cost nulls last) cost_rank
    from filtered
    where order_id is not null
),
base as (
    select
        id,
        order_id,
        status_prefix,
        case when cost_rank >= 7 then 'High' when cost_rank >= 4 then 'Medium' else 'Low' end cost_rank_level,
        greatest(cost_bucket, cost_rank) greatest_cost_bucket_cost_rank
    from with_windows
    where order_id is not null
      and cost_rank between 5 and 8
),
remaining_keys as (
    select id, order_id
    from base
    except
    select id, order_id
    from shipments
    where delivered_date <= '2024-04-01'
),
kept_rows as (
    select s.*
    from base s
    inner join remaining_keys k on s.id = k.id and s.order_id = k.order_id
)
select
    case when greatest_cost_bucket_cost_rank >= 89 then 'LARGE' else 'SMALL' end as greatest_cost_bucket_cost_rank_tier,
    count(*) as num_rows,
    percentile_approx(greatest_cost_bucket_cost_rank, array(0.25, 0.5, 0.75)) as greatest_cost_bucket_cost_rank_quartiles
from kept_rows
group by rollup (case when greatest_cost_bucket_cost_rank >= 89 then 'LARGE' else 'SMALL' end)
having count(*) > 1
