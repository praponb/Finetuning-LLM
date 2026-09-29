WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM payments
),
sessionized AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
SELECT
    id,
    amount,
    paid_at,
    status,
    island_id,
    CASE WHEN status RLIKE '^(SUCCESS|REFUNDED)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS status_zone
FROM sessionized
ORDER BY island_id DESC;
