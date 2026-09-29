with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        explode(coupon_codes) as element
    from carts
),
enriched as (
    -- Attach customers attributes
    select
        src.*,
        c.city,
        c.segment
    from exploded src
    inner join customers c
        on src.customer_id <=> c.id
    where c.email like '%@GMAIL.COM'
),
enriched_carts as (
    -- Attach customers attributes
    select
        s.customer_id,
        s.created_at,
        s.element,
        s.city,
        b.country,
        b.segment as customer_segment,
        b.city as customer_city
    from enriched s
    left join customers b
        on s.customer_id <=> b.id
),
joined as (
    -- Attach customers attributes
    select
        a.*,
        c.credit_limit,
        c.signup_date
    from enriched_carts a
    inner join customers c
        on a.customer_id = c.id
),
enriched_carts_2 as (
    -- Attach customers attributes
    select
        e.*,
        d.segment,
        d.signup_date as customer_signup_date,
        d.credit_limit as customer_credit_limit
    from joined e
    left join customers d
        on e.customer_id <=> d.id
),
remaining_keys as (
    -- Set intersection on keys
    select customer_id
    from enriched_carts_2
    intersect
    select customer_id
    from carts
    where size(coupon_codes) >= 2
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched_carts_2 s
    inner join remaining_keys k on s.customer_id = k.customer_id
),
scored as (
    -- Running and moving aggregates
    select
        element,
        city,
        credit_limit,
        customer_credit_limit,
        round(customer_credit_limit / sum(customer_credit_limit) over (partition by month(created_at)), 4) as customer_credit_limit_share
    from reconciled
)
-- Native SQL PIVOT
select *
from (
    select
        city,
        element,
        credit_limit
    from scored
) src
pivot (
    avg(credit_limit) as value_sum,
    count(1) as item_count
    for element in ('WELCOME5', 'BUNDLE', 'SPRING10')
)
