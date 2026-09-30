SELECT
    id,
    city,
    credit_limit,
    RANK() OVER (PARTITION BY city ORDER BY credit_limit DESC) AS rnk
FROM customers;
