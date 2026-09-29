with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        element
    from carts
    lateral view explode(coupon_codes) t as element
),
enriched as (
    -- Attach customers attributes
    select src.*, p.signup_date
    from flattened src
    left join customers p
        on src.customer_id = p.id
    where p.segment = 'SMB'
),
enriched_carts as (
    -- Attach customers attributes
    select
        e.id,
        e.customer_id,
        e.element,
        e.signup_date,
        p.city,
        p.credit_limit,
        p.email
    from enriched e
    left join customers p
        on e.customer_id <=> p.id
),
joined as (
    -- Attach customers attributes
    select
        s.customer_id,
        s.element,
        s.signup_date,
        s.credit_limit,
        s.email,
        p.country,
        p.name,
        p.segment
    from enriched_carts s
    inner join customers p
        on s.customer_id = p.id
),
joined_carts as (
    -- Attach customers attributes
    select
        src.customer_id,
        src.signup_date,
        src.credit_limit,
        c.age
    from joined src
    left join customers c
        on src.customer_id = c.id
),
summary as (
    -- Multi-level aggregation
    select
        customer_id,
        avg(credit_limit) as avg_credit_limit,
        count(*) as cnt,
        min(credit_limit) as min_credit_limit,
        percentile_approx(age, 0.95) as age_p95,
        count(distinct customer_id) as distinct_customer_ids,
        round(avg(credit_limit), 2) as avg_credit_limit_2
    from joined_carts
    group by rollup (customer_id)
)
select
    customer_id,
    avg_credit_limit,
    cnt,
    min_credit_limit,
    age_p95,
    distinct_customer_ids,
    avg_credit_limit_2
from summary
order by avg_credit_limit desc nulls first;
