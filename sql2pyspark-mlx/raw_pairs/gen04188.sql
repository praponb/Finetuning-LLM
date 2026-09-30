with flattened as (
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        attr_value
    from carts
    lateral view explode(attributes) x as key, attr_value
),
exploded as (
    select
        id,
        customer_id,
        created_at,
        key,
        attr_value,
        code
    from flattened
    lateral view explode(coupon_codes) e as code
),
enriched as (
    select
        a.customer_id,
        a.created_at,
        a.key,
        a.code,
        c.age,
        c.country,
        c.segment
    from exploded a
    left join customers c
        on a.customer_id <=> c.id
    where c.name is not null
),
with_dims as (
    select
        s.customer_id,
        s.created_at,
        s.key,
        s.code,
        s.country,
        b.age as customer_age,
        b.email
    from enriched s
    join customers b
        on s.customer_id = b.id
),
enriched_carts as (
    select
        src.*,
        p.credit_limit,
        p.name,
        p.signup_date
    from with_dims src
    join customers p
        on src.customer_id <=> p.id
),
enriched_carts_2 as (
    select
        e.*,
        c.credit_limit as customer_credit_limit,
        c.signup_date as customer_signup_date
    from enriched_carts e
    left join customers c
        on e.customer_id = c.id
),
scored as (
    select
        customer_id,
        key,
        credit_limit,
        name,
        signup_date,
        max(customer_credit_limit) over (partition by country, customer_id order by cast(created_at as bigint) range between 3600 preceding and current row) customer_credit_limit_max_1h,
        count(customer_age) over (order by cast(created_at as bigint) range between 3600 preceding and current row) customer_age_count_1h,
        round(credit_limit / sum(credit_limit) over (partition by date_trunc('WEEK', customer_signup_date)), 4) credit_limit_share
    from enriched_carts_2
)
select
    d.city,
    src.key,
    count(*) as cnt,
    round(sum(src.credit_limit), 2) as total_credit_limit,
    grouping_id() as level_id
from scored src
join customers d on src.customer_id = d.id
group by rollup (d.city, src.key)
order by key desc nulls last
