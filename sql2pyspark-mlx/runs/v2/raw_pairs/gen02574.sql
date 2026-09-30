SELECT account_type, MIN(opened_date) AS min_opened_date FROM accounts WHERE customer_id IN (22, 35) AND status IN ('CLOSED', 'FROZEN') GROUP BY account_type;
