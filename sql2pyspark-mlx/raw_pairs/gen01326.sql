with cleaned as (
    -- Keep recent, valid rows
    select *, ceil(monthly_fee) as monthly_fee_whole
    from subscriptions
    where customer_id is not null
),
kept_keys as (
    -- Set difference on keys
    select id, customer_id
    from cleaned
    except
    select id, customer_id
    from subscriptions
    where status in ('ACTIVE', 'PAUSED')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join kept_keys k on s.id = k.id and s.customer_id = k.customer_id
),
enriched as (
    -- Running and moving aggregates
    select
        customer_id,
        monthly_fee,
        status,
        monthly_fee_whole,
        min(monthly_fee) over (partition by plan) as plan_min_monthly_fee,
        dense_rank() over (partition by customer_id, plan order by monthly_fee_whole nulls first) as monthly_fee_whole_dense_rank
    from verified
),
enriched_subscriptions as (
    -- Ranking and gap features
    select
        customer_id,
        monthly_fee,
        plan_min_monthly_fee,
        monthly_fee_whole_dense_rank,
        cume_dist() over (order by monthly_fee desc nulls first) as monthly_fee_cume_dist
    from enriched
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        customer_id,
        monthly_fee_cume_dist,
        monthly_fee
    from enriched_subscriptions
) src
unpivot (
    reading for measure in (monthly_fee_cume_dist, monthly_fee as MONTHLY_FEE)
)
