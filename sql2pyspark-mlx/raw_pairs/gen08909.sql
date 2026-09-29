with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        round(monthly_fee * 1.07, 2) adjusted_monthly_fee
    from subscriptions
    where end_date > date '2023-06-21'
      and customer_id is not null
)
-- Join a dimension and aggregate
select
    p.country,
    src.status,
    count(*) as num_rows,
    max(src.adjusted_monthly_fee) as max_adjusted_monthly_fee,
    max(src.monthly_fee) as max_monthly_fee,
    grouping_id() as grouping_level
from base src
left join customers p on src.customer_id = p.id
group by cube (p.country, src.status)
