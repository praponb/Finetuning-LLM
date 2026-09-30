WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ARRAY_JOIN(tags, '|') AS tags_text,
        SORT_ARRAY(tags) AS sorted_tags
    FROM products
    WHERE launch_date > DATE '2023-09-15'
      AND supplier_id IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM filtered
    QUALIFY RANK() OVER (PARTITION BY category ORDER BY stock_qty DESC NULLS FIRST) = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.product_id = s.id
              AND ch.last_restock < s.launch_date
        )
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.category,
        s.price,
        s.cost,
        s.launch_date,
        s.tags,
        s.supplier_id,
        s.tags_text,
        s.sorted_tags,
        latest.store_sale_units,
        latest.sale_date
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.units AS store_sale_units, c.sale_date
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged_products AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.product_id IS NOT NULL
        )
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(product_name, 'Unknown') AS product_name_filled
    FROM not_flagged_products
    WHERE launch_date > TO_DATE('2023-03-21')
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', launch_date) AS year_start,
    STDDEV_SAMP(cost) AS cost_stddev,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(price, ARRAY(0.25, 0.5, 0.75)) AS price_quartiles,
    COUNT_IF(category = 'Sports') AS sports_events,
    PERCENTILE_APPROX(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles
FROM base
GROUP BY DATE_TRUNC('YEAR', launch_date)
ORDER BY distinct_ids DESC NULLS FIRST
