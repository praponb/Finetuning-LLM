WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(quantity, 0) AS quantity_filled
    FROM order_items
    WHERE product_id IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT id, order_id
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price DESC, id) AS rank_in_group
        FROM base
    ) t
    WHERE rank_in_group = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT
        a.*,
        d.channel,
        d.status
    FROM ranked a
    JOIN orders d
        ON a.order_id = d.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(status, 1, 3) AS status_prefix
    FROM enriched
    WHERE status IS DISTINCT FROM 'PENDING'
      AND status IS NOT NULL
)
-- Grouped metrics
SELECT
    order_id,
    COUNT(DISTINCT status) AS distinct_status,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT(*) AS row_count,
    COUNT_IF(channel = 'web') AS web_events
FROM valid
GROUP BY order_id
HAVING web_events > 1
ORDER BY num_status DESC
