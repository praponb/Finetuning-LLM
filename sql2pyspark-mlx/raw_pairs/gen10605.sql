SELECT account_type, COUNT(DISTINCT balance) AS unique_statuss, COUNT(CASE WHEN status = 'FROZEN' THEN 1 END) AS frozen_count, COUNT(*) AS row_count FROM accounts GROUP BY account_type
