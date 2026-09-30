WITH src AS (
    SELECT *
    FROM payments
    WHERE order_id IN (1, 4) AND amount BETWEEN 760 AND 1160
),
PaymentsStats AS (
    SELECT CASE WHEN method IN ('cash', 'bank_transfer') THEN 'tier_1' ELSE 'other' END AS method_group, ROUND((100 * SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_failed
    FROM src
    GROUP BY CASE WHEN method IN ('cash', 'bank_transfer') THEN 'tier_1' ELSE 'other' END
)
SELECT method_group, pct_failed
FROM PaymentsStats
