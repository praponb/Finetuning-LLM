select a.id, a.customer_id from accounts a where a.balance between 20000 and 47000 and a.customer_id in (select id from customers where length(email) < 14) order by a.id desc, customer_id desc
