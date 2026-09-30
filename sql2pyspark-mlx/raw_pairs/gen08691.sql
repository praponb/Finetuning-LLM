SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY segment ORDER BY signup_date DESC) AS pos FROM customers) AS ranked WHERE pos <= 3 ORDER BY signup_date;
