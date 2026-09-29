SELECT
    id,
    city,
    credit_limit,
    segment
FROM (
    SELECT
        id,
        city,
        credit_limit,
        segment,
        RANK() OVER (PARTITION BY city ORDER BY credit_limit DESC) AS rnk
    FROM customers
    WHERE email LIKE '%@company.co'
) AS r
WHERE rnk = 1;
