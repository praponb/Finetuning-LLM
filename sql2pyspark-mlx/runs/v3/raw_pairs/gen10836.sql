select
    order_items.unit_price,
    products.category,
    suppliers.city,
    suppliers.country,
    round(products.cost * 0.9, 2) as cost_adjusted
from order_items
join products on order_items.product_id = products.id
join suppliers on products.supplier_id = suppliers.id
order by cost_adjusted desc;
