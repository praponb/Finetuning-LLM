with with_last as (
    select
        s.id,
        s.store_id,
        s.discount,
        s.shipping_fee,
        latest.order_item_unit_price
    from orders s
    left join lateral (
            select c.unit_price as order_item_unit_price
            from order_items c
            where c.order_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) latest on true
)
select
    s.id,
    s.store_id,
    s.shipping_fee,
    (select min(t2.units) from store_sales t2 where t2.store_id = s.store_id) as min_store_sale_units
from with_last s
order by shipping_fee desc, store_id asc;
