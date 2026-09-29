SELECT * FROM (SELECT merchant, SUM(CASE WHEN txn_type = 'FEE' THEN 1 ELSE 0 END) AS fee_count FROM transactions GROUP BY merchant) AS x WHERE fee_count < 56
