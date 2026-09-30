select id, product_id, quantity, row_number() over (partition by product_id order by quantity desc, id) as rn from inventory;
