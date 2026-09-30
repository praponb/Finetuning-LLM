select id, store_id, quantity from inventory qualify row_number() over (partition by product_id, store_id order by quantity desc, id) = 1;
