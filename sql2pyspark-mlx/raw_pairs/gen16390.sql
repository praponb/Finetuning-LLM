SELECT id, segment, signup_date FROM (SELECT *, RANK() OVER (PARTITION BY segment ORDER BY signup_date) rnk FROM customers) t WHERE rnk = 1
