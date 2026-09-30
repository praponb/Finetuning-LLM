WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        unit_price - quantity AS line_total
    FROM order_items
    WHERE quantity > 7
),
base_order_items AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, line_total - unit_price AS line_total_minus_unit_price
    FROM base
    WHERE line_total < 32
),
kept_keys AS (
    -- Set intersection on keys
    SELECT order_id, product_id
    FROM base_order_items
    INTERSECT ALL
    SELECT order_id, product_id
    FROM order_items
    WHERE product_id IS NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base_order_items s
    INNER JOIN kept_keys k ON s.order_id = k.order_id AND s.product_id = k.product_id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY unit_price DESC NULLS FIRST) AS unit_price_percent_rank,
        CUME_DIST() OVER (PARTITION BY product_id, order_id ORDER BY line_total ASC) AS line_total_cume_dist,
        CUME_DIST() OVER (ORDER BY line_total DESC) AS line_total_cume_dist_2,
        SUM(line_total) OVER (PARTITION BY order_id) AS order_id_sum_line_total
    FROM kept_rows
    WHERE line_total_minus_unit_price BETWEEN 47 AND 58
)
-- Join a dimension and aggregate
SELECT
    p.brand,
    COUNT(*) AS cnt,
    SUM(a.unit_price_percent_rank) AS total_unit_price_percent_rank,
    AVG(a.line_total_cume_dist_2) AS avg_line_total_cume_dist_2
FROM enriched a
INNER JOIN products p ON a.product_id = p.id
GROUP BY ROLLUP (p.brand);
