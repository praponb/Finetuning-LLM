select o.id, o.store_id, o.status, o.customer_id from orders o where o.customer_id in (select id from customers where upper(email) like '%ARO%') order by status asc;
