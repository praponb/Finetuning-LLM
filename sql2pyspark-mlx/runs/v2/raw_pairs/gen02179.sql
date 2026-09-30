select i.id, i.store_id, i.quantity from inventory i where i.store_id not in (select id from stores where store_name like '%Flagship%')
