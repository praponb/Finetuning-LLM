select a.id, a.balance, c.country from accounts a left join customers c on a.customer_id = c.id where length(c.name) > 5 order by country;
