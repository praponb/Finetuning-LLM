with lines as (
    select
        id,
        customer_id,
        code
    from carts
    lateral view outer explode(coupon_codes) t as code
),
enriched as (
    select
        e.id,
        e.customer_id,
        d.name
    from lines e
    inner join customers d
        on e.customer_id = d.id
),
remaining_keys as (
    select customer_id, id
    from enriched
    except all
    select customer_id, id
    from carts
    where hour(created_at) between 6 and 13
),
kept_rows as (
    select s.*
    from enriched s
    inner join remaining_keys k on s.customer_id = k.customer_id and s.id = k.id
),
enriched_carts as (
    select
        e.id,
        e.customer_id,
        d.credit_limit,
        d.signup_date,
        d.email
    from kept_rows e
    left join customers d
        on e.customer_id = d.id
    where d.city not in ('Tokyo', 'Chiang Mai', 'Berlin')
),
enriched_carts_2 as (
    select
        s.*,
        b.email as customer_email,
        b.country
    from enriched_carts s
    left join customers b
        on s.customer_id <=> b.id
)
select
    *,
    stddev_samp(credit_limit) over (partition by month(signup_date), customer_id) as signup_date_month_stddev_samp_credit_limit,
    round(credit_limit / sum(credit_limit) over (partition by country), 4) as credit_limit_share
from enriched_carts_2
