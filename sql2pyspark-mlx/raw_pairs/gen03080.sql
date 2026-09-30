WITH numbered AS (
    SELECT
        id,
        method,
        amount,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) AS rnk
    FROM payments
)
SELECT *
FROM numbered
WHERE rnk <= 1;
