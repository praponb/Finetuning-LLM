WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at NULLS FIRST, id) AS row_num
        FROM payments
    ) t
    WHERE row_num = 1
)
SELECT
    id,
    method,
    amount,
    paid_at,
    status,
    row_num,
    CASE WHEN status RLIKE '^(REFUNDED|FAILED)$' THEN 'PRIORITY' WHEN status RLIKE '^(SUCCESS)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS status_zone
FROM deduped
ORDER BY paid_at DESC, status
