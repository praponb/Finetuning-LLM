WITH dim_totals AS (
    SELECT
        p.country,
        a.status,
        COUNT(*) AS cnt,
        ROUND(AVG(a.balance), 2) AS avg_balance
    FROM accounts a
    INNER JOIN customers p ON a.customer_id = p.id
    GROUP BY ROLLUP (p.country, a.status)
)
SELECT
    status,
    cnt,
    CASE WHEN status RLIKE '^(CLOSED|FROZEN)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS status_zone
FROM dim_totals
ORDER BY cnt DESC NULLS FIRST;
