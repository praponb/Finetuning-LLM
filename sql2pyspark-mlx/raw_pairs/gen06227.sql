WITH Ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date, id) AS rn FROM customers) SELECT id, country, signup_date, city FROM Ranked WHERE rn = 1 ORDER BY country DESC;
