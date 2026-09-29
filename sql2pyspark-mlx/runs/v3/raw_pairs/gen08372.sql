SELECT status, COALESCE(status, 'N/A') AS status_filled FROM accounts WHERE status IN ('CLOSED', 'FROZEN', 'OPEN');
