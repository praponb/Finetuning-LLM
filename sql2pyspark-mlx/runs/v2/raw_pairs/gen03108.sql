SELECT id, status, channel, CASE WHEN status = 'RETURNED' THEN 1 ELSE 0 END AS is_returned FROM orders WHERE status = 'PENDING' AND (discount < 0.17 OR amount IS NOT NULL);
