WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY status ORDER BY amount DESC NULLS LAST) AS dedup_rank
    FROM payments
    QUALIFY dedup_rank = 1
)
SELECT
    id,
    order_id,
    method,
    amount,
    paid_at,
    status,
    dedup_rank,
    CASE WHEN status RLIKE '^(SUCCESS|FAILED)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS status_zone
FROM deduped
ORDER BY status_zone DESC;
