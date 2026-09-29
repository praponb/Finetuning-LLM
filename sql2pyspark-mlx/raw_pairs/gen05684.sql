select
    p.stock_qty,
    p.brand,
    p.launch_date,
    oi.unit_price
from products p
join order_items oi on p.id = oi.product_id
where p.price between 840 and 900
order by unit_price desc;
