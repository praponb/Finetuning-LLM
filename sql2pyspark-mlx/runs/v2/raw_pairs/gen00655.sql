WITH ranked AS (SELECT id, segment, signup_date, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date, id) AS rnk FROM customers) SELECT id, segment, signup_date FROM ranked WHERE rnk <= 2;
