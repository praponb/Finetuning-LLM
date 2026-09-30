SELECT c.is_active, MIN(a.opened_date) AS min_opened_date FROM accounts a INNER JOIN customers c ON c.id = a.customer_id WHERE a.balance BETWEEN 24000 AND 46000 GROUP BY c.is_active;
