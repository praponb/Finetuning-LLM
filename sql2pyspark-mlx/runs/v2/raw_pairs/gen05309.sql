SELECT id, plan, end_date FROM (SELECT *, RANK() OVER (PARTITION BY plan ORDER BY end_date) AS row_num FROM subscriptions) AS r WHERE row_num <= 3 ORDER BY plan
