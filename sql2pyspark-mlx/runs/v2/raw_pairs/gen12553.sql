select a.id, a.customer_id, a.opened_date, c.country from accounts a inner join customers c on a.customer_id = c.id where c.age <= 56 and a.customer_id = 27;
