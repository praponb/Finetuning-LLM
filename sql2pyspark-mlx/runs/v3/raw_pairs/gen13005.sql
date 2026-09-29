SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date DESC, id) AS rn FROM subscriptions WHERE status = 'CANCELLED') AS r WHERE rn <= 2 ORDER BY rn DESC, plan ASC;
