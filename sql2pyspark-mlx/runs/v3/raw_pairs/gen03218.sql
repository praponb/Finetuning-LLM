SELECT DATE_TRUNC('MONTH', opened_date) opened_month, AVG(balance) avg_balance FROM accounts WHERE customer_id IN (24, 27, 29) OR customer_id IS NOT NULL GROUP BY 1;
