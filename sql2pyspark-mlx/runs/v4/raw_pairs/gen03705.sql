SELECT *, ABS(balance) abs_balance, DATE_TRUNC('week', opened_date) opened_date_period FROM accounts WHERE customer_id IN (11, 35) ORDER BY balance DESC, id LIMIT 50;
