with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        coupon_codes,
        line.qty
    from carts
    lateral view outer explode(items) x as line
),
flattened as (
    -- Unnest the array into one row per element
    select
        customer_id,
        qty,
        tag
    from exploded
    lateral view outer explode(coupon_codes) e as tag
),
enriched as (
    -- Ranking and gap features
    select *, cume_dist() over w_recent as qty_cume_dist
    from flattened
    window w_recent as (partition by tag order by qty desc)
),
with_windows as (
    -- Window metrics over several different partitions
    select *, stddev_samp(qty) over (partition by customer_id) as customer_id_stddev_samp_qty
    from enriched
),
joined as (
    -- Attach customers attributes
    select
        a.customer_id,
        a.qty,
        a.qty_cume_dist,
        d.segment,
        d.signup_date,
        d.country
    from with_windows a
    left join customers d
        on a.customer_id = d.id
    where d.country not in ('DE', 'TH')
),
enriched_carts as (
    -- Attach customers attributes
    select
        s.customer_id,
        s.signup_date,
        s.country,
        c.credit_limit,
        c.age
    from joined s
    left join customers c
        on s.customer_id <=> c.id
)
select
    s.customer_id,
    s.signup_date,
    s.credit_limit,
    s.age,
    (select avg(x.monthly_fee) from subscriptions x where x.customer_id = s.customer_id) as avg_subscription_monthly_fee
from enriched_carts s
order by age desc nulls first;
