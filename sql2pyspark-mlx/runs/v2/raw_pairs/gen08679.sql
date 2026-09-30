SELECT opened_date FROM accounts WHERE balance IS NOT NULL UNION ALL SELECT opened_date FROM accounts WHERE status IS NULL
