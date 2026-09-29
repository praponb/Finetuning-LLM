WITH Ranked AS (SELECT *, DENSE_RANK() OVER (PARTITION BY segment ORDER BY signup_date DESC) AS pos FROM customers) SELECT * FROM Ranked WHERE pos <= 5 ORDER BY signup_date
