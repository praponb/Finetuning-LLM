SELECT id, plan, end_date FROM (SELECT *, RANK() OVER (PARTITION BY plan ORDER BY end_date DESC) AS row_num FROM subscriptions) AS t WHERE row_num = 1 ORDER BY id DESC
