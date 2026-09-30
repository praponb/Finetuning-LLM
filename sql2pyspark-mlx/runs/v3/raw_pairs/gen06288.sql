select a.id, a.opened_date from accounts a where a.customer_id in (select id from customers where length(name) > 13) order by id
