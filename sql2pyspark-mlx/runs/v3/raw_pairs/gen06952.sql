WITH base AS (
    SELECT *, CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END AS is_tokyo
    FROM customers
    WHERE UPPER(TRIM(name)) = 'FRANK MILLER' AND city IS NOT NULL AND city IS NOT NULL
),
agg AS (
    SELECT
        city,
        segment,
        MAX(is_tokyo) AS max_is_tokyo,
        MAX(credit_limit) AS max_credit_limit
    FROM base
    GROUP BY city, segment
)
SELECT
    city,
    segment,
    max_is_tokyo,
    max_credit_limit
FROM agg
WHERE max_credit_limit < 9600
ORDER BY city DESC
