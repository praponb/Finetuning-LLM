with lines as (
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        value
    from carts
    lateral view explode(attributes) e as attr_key, value
)
select
    s.id,
    s.customer_id,
    s.created_at,
    s.attr_key,
    s.value,
    (select avg(s2.monthly_fee) from subscriptions s2 where s2.customer_id = s.customer_id) as avg_subscription_monthly_fee
from lines s
order by attr_key, created_at desc;
