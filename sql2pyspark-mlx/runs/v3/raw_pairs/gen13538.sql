select o.id, o.status, o.channel from orders o where o.customer_id in (21, 38) and o.store_id not in (select id from stores where opened_date > '2024-07-01') order by status desc, channel asc
