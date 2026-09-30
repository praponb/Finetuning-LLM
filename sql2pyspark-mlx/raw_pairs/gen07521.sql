WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        GREATEST(unit_price, quantity) AS greatest_unit_price_quantity
    FROM order_items
    WHERE product_id IS NOT NULL
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY greatest_unit_price_quantity DESC, id) AS row_num
    FROM cleaned
    QUALIFY row_num = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        unit_price,
        greatest_unit_price_quantity,
        row_num,
        PERCENT_RANK() OVER w_part AS greatest_unit_price_quantity_percent_rank,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY unit_price DESC) AS unit_price_dense_rank,
        ROW_NUMBER() OVER w_hist AS row_seq,
        NTILE(10) OVER (ORDER BY unit_price DESC, id) AS unit_price_bucket
    FROM latest
    WHERE unit_price BETWEEN 120 AND 250
    WINDOW w_part AS (PARTITION BY order_id, product_id ORDER BY greatest_unit_price_quantity DESC), w_hist AS (PARTITION BY product_id ORDER BY unit_price, id)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY unit_price_bucket DESC NULLS LAST) AS rank_in_group
    FROM metrics
    QUALIFY rank_in_group = 1
),
daily AS (
    -- Multi-level aggregation
    SELECT
        product_id,
        AVG(unit_price_bucket) AS avg_unit_price_bucket,
        APPROX_PERCENTILE(greatest_unit_price_quantity_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS greatest_unit_price_quantity_percent_rank_quartiles,
        COUNT(*) AS row_count
    FROM ranked
    GROUP BY 1
    HAVING AVG(unit_price_bucket) > 0
)
SELECT
    s.product_id,
    s.avg_unit_price_bucket,
    s.row_count,
    (SELECT MIN(o2.unit_price) FROM order_items o2 WHERE o2.product_id = s.product_id) AS min_order_item_unit_price
FROM daily s;
