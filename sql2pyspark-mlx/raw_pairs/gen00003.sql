WITH by_dimension AS (
    SELECT
        d.status,
        d.account_type,
        COUNT(*) AS cnt,
        MAX(s.amount) AS max_amount
    FROM transactions s
    INNER JOIN accounts d ON s.account_id = d.id
    GROUP BY ROLLUP (d.status, d.account_type)
)
SELECT
    status,
    cnt,
    max_amount
FROM by_dimension;
