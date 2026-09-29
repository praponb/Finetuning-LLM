SELECT
    id,
    country,
    signup_date,
    RANK() OVER (PARTITION BY country ORDER BY signup_date DESC) AS rnk
FROM customers
ORDER BY id DESC;
