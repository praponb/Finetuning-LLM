SELECT id, customer_id, balance FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance, id) AS rnk FROM accounts WHERE opened_date <= '2023-04-12') AS r WHERE rnk <= 3;
