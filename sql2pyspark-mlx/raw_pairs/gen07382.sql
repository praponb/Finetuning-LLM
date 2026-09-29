WITH dim_totals AS (
    SELECT
        d.city,
        d.segment,
        COUNT(*) AS cnt,
        MIN(s.balance) AS min_balance,
        GROUPING_ID() AS grouping_level
    FROM accounts s
    INNER JOIN customers d ON s.customer_id = d.id
    GROUP BY GROUPING SETS ((d.city, d.segment), (d.city), ())
)
SELECT
    city,
    segment,
    min_balance
FROM dim_totals
ORDER BY min_balance DESC, city DESC
