select
    oi.id,
    oi.unit_price,
    oi.product_id,
    p.brand
from order_items oi
join products p on oi.product_id = p.id
where oi.product_id is null and oi.unit_price > 380;
