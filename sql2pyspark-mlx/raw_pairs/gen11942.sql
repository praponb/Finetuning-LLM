WITH cleaned AS (
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        COALESCE(unit_price, 0) AS unit_price_filled,
        FLOOR(unit_price) AS unit_price_whole
    FROM order_items
    WHERE product_id IS NOT NULL
),
metrics AS (
    SELECT
        id,
        order_id,
        quantity,
        unit_price_whole,
        SUM(unit_price_whole) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price_whole,
        LAST_VALUE(unit_price_whole) IGNORE NULLS OVER (ORDER BY unit_price_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price_whole,
        ROW_NUMBER() OVER (ORDER BY unit_price_whole DESC, id) AS row_seq,
        NTILE(10) OVER (PARTITION BY order_id ORDER BY unit_price_whole, id) AS unit_price_whole_bucket,
        AVG(unit_price_filled) OVER (PARTITION BY order_id ORDER BY unit_price_filled, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS unit_price_filled_avg_2_prior
    FROM cleaned
),
kept_keys AS (
    SELECT id
    FROM metrics
    EXCEPT DISTINCT
    SELECT id
    FROM order_items
    WHERE unit_price BETWEEN 120 AND 220
),
kept_rows AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN kept_keys k ON s.id = k.id
),
cleaned_order_items AS (
    SELECT
        order_id,
        quantity,
        last_known_unit_price_whole,
        row_seq,
        CAST(row_seq AS DOUBLE) AS row_seq_str
    FROM kept_rows
    WHERE quantity BETWEEN 3 AND 6
)
SELECT
    *,
    ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY order_id), 4) AS row_seq_share,
    SUM(quantity) OVER (PARTITION BY order_id) AS order_id_sum_quantity,
    DENSE_RANK() OVER (PARTITION BY order_id ORDER BY row_seq_str DESC) AS row_seq_str_dense_rank,
    RANK() OVER (ORDER BY row_seq NULLS FIRST) AS row_seq_rank
FROM cleaned_order_items;
