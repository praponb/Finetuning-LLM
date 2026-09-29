with top_inventory as (select *, row_number() over (partition by product_id order by quantity, id) as pos from inventory) select id, product_id, quantity, store_id from top_inventory where pos <= 5;
