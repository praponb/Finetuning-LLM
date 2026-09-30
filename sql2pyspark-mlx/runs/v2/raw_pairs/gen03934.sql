SELECT merchant, credit_count FROM (SELECT merchant, SUM(CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END) AS credit_count FROM transactions GROUP BY merchant) AS sub WHERE credit_count > 45;
