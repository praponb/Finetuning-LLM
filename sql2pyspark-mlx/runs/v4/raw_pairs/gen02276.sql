WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier, order_id ORDER BY cost DESC NULLS FIRST, id) AS latest_rank
    FROM shipments
    QUALIFY latest_rank = 1
)
SELECT
    id,
    delivered_date,
    cost,
    status,
    latest_rank,
    CASE WHEN status RLIKE '^(LOST|RETURNED)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS status_zone
FROM ranked
ORDER BY status DESC NULLS LAST, status_zone
