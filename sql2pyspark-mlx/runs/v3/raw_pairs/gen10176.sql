SELECT COUNT(DISTINCT balance) AS unique_customers, ROUND(STDDEV(balance), 2) AS stddev_balance FROM accounts WHERE account_type IN ('SAVINGS', 'CHECKING')
