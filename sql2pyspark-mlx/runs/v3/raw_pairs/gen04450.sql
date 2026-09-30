SELECT opened_date FROM accounts WHERE status IN ('CLOSED', 'FROZEN') AND customer_id IN (7, 9) AND status = 'OPEN';
