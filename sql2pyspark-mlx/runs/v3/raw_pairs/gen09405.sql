SELECT *
FROM (
    SELECT
        id,
        segment,
        credit_limit,
        email,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit DESC, id) AS rn
    FROM customers
    WHERE credit_limit BETWEEN 6900 AND 14900
) AS ranked
WHERE rn <= 3;
