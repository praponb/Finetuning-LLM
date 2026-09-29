SELECT id, plan, end_date FROM (SELECT id, plan, end_date, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date, id) AS rn FROM subscriptions) AS t WHERE rn <= 3 ORDER BY plan ASC, end_date DESC
