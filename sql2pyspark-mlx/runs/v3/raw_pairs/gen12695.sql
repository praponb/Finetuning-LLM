SELECT opened_date, balance, status
FROM accounts
WHERE (customer_id IS NOT NULL AND status = 'OPEN') OR status NOT IN ('CLOSED', 'OPEN')
