WITH by_dimension AS (
    SELECT
        dim.segment,
        src.status,
        COUNT(*) AS cnt,
        ROUND(AVG(src.monthly_fee), 2) AS avg_monthly_fee,
        GROUPING_ID() AS grouping_level
    FROM subscriptions src
    JOIN customers dim ON src.customer_id = dim.id
    GROUP BY GROUPING SETS ((dim.segment, src.status), (dim.segment), ())
)
SELECT
    status,
    avg_monthly_fee,
    grouping_level
FROM by_dimension
ORDER BY status;
