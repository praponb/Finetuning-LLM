with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        explode(coupon_codes) as tag
    from carts
),
joined as (
    -- Attach customers attributes
    select
        s.*,
        d.email,
        d.segment,
        d.age
    from flattened s
    join customers d
        on s.customer_id = d.id
    where d.is_active = true
),
joined_carts as (
    -- Attach customers attributes
    select src.*, p.credit_limit
    from joined src
    left join customers p
        on src.customer_id = p.id
),
with_dims as (
    -- Attach customers attributes
    select e.*, b.name
    from joined_carts e
    join customers b
        on e.customer_id <=> b.id
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        created_at,
        email,
        segment,
        age,
        credit_limit,
        name,
        age - credit_limit as age_minus_credit_limit
    from with_dims
    where created_at >= to_date('2025-03-01')
      and created_at < '2024-01-01'
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        percent_rank() over (partition by segment order by age_minus_credit_limit asc) as age_minus_credit_limit_percent_rank,
        sum(credit_limit) over (partition by year(created_at), segment order by cast(created_at as bigint) asc range between 3600 preceding and current row) as credit_limit_sum_1h,
        dense_rank() over (partition by segment order by age_minus_credit_limit desc) as age_minus_credit_limit_dense_rank,
        sum(credit_limit) over w_running as credit_limit_sum_2h
    from cleaned
    window w_running as (partition by segment order by cast(created_at as bigint) asc range between 7200 preceding and current row)
),
scored as (
    -- Window metrics over several different partitions
    select *, stddev_samp(age_minus_credit_limit_percent_rank) over (partition by segment) as segment_stddev_samp_age_minus_credit_limit_percent_rank
    from enriched
)
select
    created_at,
    email,
    segment,
    age,
    credit_limit,
    name,
    age_minus_credit_limit,
    age_minus_credit_limit_percent_rank,
    credit_limit_sum_1h
from scored
order by segment desc nulls first, age asc;
