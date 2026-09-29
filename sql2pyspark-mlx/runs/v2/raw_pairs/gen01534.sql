select product_id, min_quantity from (select product_id, min(quantity) as min_quantity from order_items group by product_id) as s where min_quantity > 5.3
