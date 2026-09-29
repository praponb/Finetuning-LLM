select id, product_id, quantity, avg(quantity) over (partition by product_id) as product_id_avg_quantity from order_items;
