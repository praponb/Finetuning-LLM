SELECT opened_date, customer_id, status, CONCAT_WS(',', account_type, status) AS account_type_status_key FROM accounts WHERE status NOT IN ('CLOSED', 'OPEN') AND status IS NOT NULL ORDER BY status, 2
