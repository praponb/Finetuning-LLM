SELECT COUNT(DISTINCT balance) AS unique_statuss FROM accounts WHERE opened_date < '2025-01-06' AND account_type = 'LOAN' ORDER BY unique_statuss;
