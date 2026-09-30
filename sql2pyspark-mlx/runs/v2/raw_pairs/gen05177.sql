select id, product_id, order_id, quantity, lead(quantity, 1) over (partition by product_id, order_id order by quantity, id) next_quantity from order_items
