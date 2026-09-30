select id, product_id, quantity, ntile(4) over (partition by product_id order by quantity, id) as quartile from order_items where unit_price >= 260;
