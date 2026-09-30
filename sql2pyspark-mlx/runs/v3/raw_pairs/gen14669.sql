SELECT id, opened_date, customer_id, DATE_SUB(opened_date, 28) AS opened_date_minus_28d FROM accounts WHERE balance BETWEEN 16000 AND 24000 AND status IN ('CLOSED', 'FROZEN');
