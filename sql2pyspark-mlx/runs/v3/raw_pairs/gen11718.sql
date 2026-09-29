SELECT QUARTER(opened_date) AS opened_quarter, ROUND(STDDEV(balance), 2) AS stddev_balance, COUNT(balance) AS balance_count FROM accounts WHERE customer_id IN (10, 35, 40) GROUP BY 1;
