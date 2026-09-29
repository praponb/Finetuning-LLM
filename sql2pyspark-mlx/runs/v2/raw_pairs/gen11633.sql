select product_id, count(distinct unit_price) as unique_products, avg(quantity) as avg_quantity from order_items where unit_price < 120 group by product_id order by avg_quantity
