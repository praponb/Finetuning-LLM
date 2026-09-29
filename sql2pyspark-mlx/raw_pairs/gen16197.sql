select p.id, p.brand, t.revenue from products p join (select product_id, sum(quantity * unit_price) as revenue from order_items group by product_id) t on p.id = t.product_id order by revenue, brand;
