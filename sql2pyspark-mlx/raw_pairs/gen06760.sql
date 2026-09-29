WITH Base AS (
    SELECT *
    FROM orders
    WHERE customer_id IN (7, 14, 31) AND status = 'PENDING' AND channel IN ('partner', 'mobile', 'store')
),
agg AS (
    SELECT channel, ROUND((100 * SUM(CASE WHEN channel = 'partner' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_partner
    FROM Base
    GROUP BY channel
)
SELECT *
FROM agg
WHERE pct_partner >= 26
ORDER BY 2 DESC, channel
LIMIT 3;
