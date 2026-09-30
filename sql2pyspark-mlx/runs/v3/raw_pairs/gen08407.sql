select id, product_id, last_restock, quantity, sum(quantity) over (partition by product_id order by last_restock, id) running_quantity from inventory;
