with exploded as (
    -- Unnest the array into one row per element
    select
        t.id,
        t.customer_id,
        t.created_at,
        pos,
        elem.product_id,
        elem.promo
    from carts t
    lateral view posexplode(t.items) li as pos, elem
)
select
    s.id,
    s.customer_id,
    s.pos,
    s.product_id,
    (select max(t2.amount) from orders t2 where t2.customer_id = s.customer_id) as max_order_amount
from exploded s;
