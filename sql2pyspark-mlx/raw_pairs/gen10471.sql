select
    s.id,
    s.store_id,
    s.order_date,
    s.discount,
    s.shipping_fee,
    x.order_item_unit_price
from orders s
join lateral (
        select c.unit_price as order_item_unit_price
        from order_items c
        where c.order_id = s.id
        order by c.quantity desc, c.id desc
        limit 1
    ) x on true;
