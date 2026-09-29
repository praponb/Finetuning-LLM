select product_id, sum(unit_price * quantity) revenue, count(*) total_count from order_items where unit_price > 260 and product_id = 27 group by product_id order by total_count
