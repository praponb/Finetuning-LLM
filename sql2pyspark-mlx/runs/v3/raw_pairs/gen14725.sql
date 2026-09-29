select id, store_id, product_id, coalesce(quantity, 0) as quantity_filled from inventory where product_id in (4, 10) and store_id in (2, 3, 5) order by id desc
