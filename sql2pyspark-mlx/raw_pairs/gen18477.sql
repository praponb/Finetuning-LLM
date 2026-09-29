WITH deduped AS (
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status
    FROM payments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) = 1
)
SELECT
    DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
    COUNT(*) FILTER (WHERE status = 'FAILED') AS failed_rows,
    COUNT(*) FILTER (WHERE status = 'REFUNDED') AS refunded_rows,
    COUNT_IF(status = 'SUCCESS') AS success_events,
    SIZE(COLLECT_SET(status)) AS num_status
FROM deduped
GROUP BY quarter_start
HAVING num_status > 1;
