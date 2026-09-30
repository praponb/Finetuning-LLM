select a.id, a.status, a.account_type from accounts a where a.customer_id in (select id from customers where upper(name) like '%RAN%') order by id;
