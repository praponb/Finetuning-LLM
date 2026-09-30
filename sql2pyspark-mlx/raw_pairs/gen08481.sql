select id, product_id, quantity, dense_rank() over (partition by product_id order by quantity desc) as quantity_dense_rank from order_items where order_id in (8, 28);
