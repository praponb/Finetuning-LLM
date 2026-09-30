select i.id, i.product_id from inventory i where i.product_id in (2, 10) and i.store_id not in (select id from stores where opened_date between '2024-10-01' and '2024-11-17')
