select product_id, avg(quantity) as avg_quantity, count(quantity) as quantity_count from order_items group by 1 having sum(quantity) > 17
