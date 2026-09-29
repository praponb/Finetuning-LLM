SELECT id, opened_date, customer_id, balance, CASE WHEN balance >= 65000 THEN 'High' WHEN balance >= 12000 THEN 'Medium' ELSE 'Low' END AS balance_band FROM accounts ORDER BY id LIMIT 1
