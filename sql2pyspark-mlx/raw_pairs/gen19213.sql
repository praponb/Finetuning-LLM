WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN channel IN ('partner', 'store') THEN 'Group A' ELSE 'Other' END channel_group,
        COALESCE(channel, 'UNKNOWN') channel_clean
    FROM orders
    WHERE store_id IS NOT NULL
      AND order_date > TO_DATE('2023-07-19')
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount, id) = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT channel, id
    FROM deduped
    EXCEPT ALL
    SELECT channel, id
    FROM orders
    WHERE customer_id = 24
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN matched_keys k ON s.channel = k.channel AND s.id = k.id
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.delivered_date < s.order_date
        )
)
-- Grouped metrics
SELECT
    CASE WHEN amount >= 1580 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    DATE_TRUNC('WEEK', order_date) AS week_start,
    COUNT(*) FILTER (WHERE channel = 'web') AS web_rows,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) AS completed_count
FROM kept
GROUP BY CASE WHEN amount >= 1580 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('WEEK', order_date)
