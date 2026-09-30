WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date DESC NULLS LAST, id) AS rn
        FROM shipments
    ) ranked
    WHERE rn = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, FLOOR(cost) AS cost_whole
    FROM latest
    WHERE delivered_date >= DATE '2023-06-15'
      AND cost >= 57
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(cost_whole) IGNORE NULLS OVER (PARTITION BY status ORDER BY cost_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost_whole,
        ROUND(cost / SUM(cost) OVER (PARTITION BY status), 4) AS cost_share
    FROM valid
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost_share NULLS LAST, id) = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY cost_share, id) AS rn_2
    FROM deduped
    QUALIFY rn_2 = 1
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', shipped_date) AS activity_month,
    CASE WHEN cost_share >= 0.87 THEN 'LARGE' ELSE 'SMALL' END AS cost_share_tier,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT(*) AS event_count,
    SUM(CASE WHEN carrier = 'Flash' THEN first_known_cost_whole ELSE 0 END) AS flash_first_known_cost_whole,
    GROUPING(DATE_TRUNC('MONTH', shipped_date)) AS gid
FROM ranked
GROUP BY ROLLUP (DATE_TRUNC('MONTH', shipped_date), CASE WHEN cost_share >= 0.87 THEN 'LARGE' ELSE 'SMALL' END);
