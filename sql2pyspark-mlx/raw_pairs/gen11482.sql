WITH dim_totals AS (
    SELECT
        dim.status AS order_status,
        a.status AS payment_status,
        COUNT(*) AS cnt,
        ROUND(AVG(a.amount), 2) AS avg_amount,
        GROUPING_ID() AS level_id
    FROM payments a
    INNER JOIN orders dim ON a.order_id = dim.id
    GROUP BY ROLLUP (dim.status, a.status)
)
SELECT
    order_status,
    payment_status,
    cnt,
    avg_amount,
    level_id
FROM dim_totals
ORDER BY cnt DESC NULLS LAST;
