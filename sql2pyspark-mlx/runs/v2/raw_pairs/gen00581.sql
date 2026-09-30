select a.id, a.customer_id, a.account_type, c.segment from accounts a inner join customers c on a.customer_id = c.id order by a.customer_id desc, a.id asc;
