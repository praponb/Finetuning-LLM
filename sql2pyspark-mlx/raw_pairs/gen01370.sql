WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        unit_price,
        MAX(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_quantity,
        MIN(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_quantity,
        COUNT(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_quantity,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_quantity,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY order_id), 4) AS unit_price_share
    FROM order_items
),
enriched AS (
    -- Attach orders attributes
    SELECT
        s.*,
        d.status,
        d.channel,
        d.amount
    FROM metrics s
    LEFT JOIN orders d
        ON s.order_id = d.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        order_id,
        last3_count_quantity,
        last3_avg_quantity,
        status
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price_share, id) AS dedup_rank
        FROM enriched
    ) ranked
    WHERE dedup_rank = 1
),
metrics_order_items AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(last3_count_quantity) OVER (PARTITION BY order_id ORDER BY last3_count_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_last3_count_quantity,
        AVG(last3_avg_quantity) OVER (PARTITION BY order_id) AS order_id_avg_last3_avg_quantity,
        DENSE_RANK() OVER (ORDER BY last3_count_quantity DESC NULLS LAST) AS last3_count_quantity_dense_rank
    FROM latest
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CEIL(last3_count_quantity) AS last3_count_quantity_whole,
        COALESCE(status, 'DIRECT') AS status_clean
    FROM metrics_order_items
    WHERE status IS NOT NULL
)
SELECT
    id,
    order_id,
    order_id_avg_last3_avg_quantity,
    last3_count_quantity_dense_rank,
    status_clean
FROM base
