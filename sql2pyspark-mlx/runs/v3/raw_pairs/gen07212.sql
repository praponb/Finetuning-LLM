select o.id, o.order_date from orders o where o.store_id not in (select id from stores where city = 'Tokyo') order by o.id desc
