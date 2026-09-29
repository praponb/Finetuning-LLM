select
    s.id,
    s.brand,
    s.stock_qty,
    s.tags,
    top1.order_item_unit_price
from products s
left join lateral (
        select c.unit_price as order_item_unit_price
        from order_items c
        where c.product_id = s.id
        order by c.unit_price desc, c.id desc
        limit 1
    ) top1 on true;
