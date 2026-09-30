WITH matched_keys AS (
    SELECT id, status
    FROM payments
    INTERSECT
    SELECT id, status
    FROM payments
    WHERE TO_DATE(paid_at) < '2023-12-01'
),
reconciled AS (
    SELECT s.*
    FROM payments s
    JOIN matched_keys k ON s.id = k.id AND s.status = k.status
)
SELECT
    CASE WHEN amount >= 1280 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    SIZE(COLLECT_SET(status)) AS num_status,
    SUM(amount) AS total_amount
FROM reconciled
GROUP BY CUBE (CASE WHEN amount >= 1280 THEN 'HIGH' ELSE 'LOW' END)
HAVING total_amount > 0
