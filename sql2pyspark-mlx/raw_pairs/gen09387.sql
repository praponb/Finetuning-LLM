select a.id, a.status, a.customer_id, a.balance, c.name from accounts a inner join customers c on a.customer_id = c.id where a.status = 'FROZEN' order by status asc, id asc limit 3;
