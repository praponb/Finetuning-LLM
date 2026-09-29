select i.id, i.product_id, i.store_id, i.quantity from inventory i where i.store_id in (select id from stores where month(opened_date) in (1, 5, 10)) order by i.store_id, product_id desc
