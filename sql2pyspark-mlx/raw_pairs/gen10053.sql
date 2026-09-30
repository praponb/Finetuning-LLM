WITH base AS (
    -- Keep recent, valid rows
    SELECT *, CAST(unit_price AS INT) AS unit_price_int
    FROM order_items
    WHERE product_id IS NOT NULL
),
with_dims AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.order_id,
        src.product_id,
        src.quantity,
        src.unit_price,
        src.unit_price_int,
        d.category,
        d.launch_date,
        d.product_name
    FROM base src
    LEFT JOIN products d
        ON src.product_id = d.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY category, order_id ORDER BY launch_date DESC) AS dedup_rank
        FROM with_dims
    ) x
    WHERE dedup_rank = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(unit_price_int, 1) OVER (PARTITION BY category, product_id ORDER BY dedup_rank, id) AS prev_unit_price_int,
        ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY launch_date, id) AS event_seq,
        NTILE(4) OVER (ORDER BY unit_price, id) AS unit_price_bucket
    FROM ranked
),
kept_keys AS (
    -- Set intersection on keys
    SELECT product_id
    FROM with_windows
    INTERSECT
    SELECT product_id
    FROM order_items
    WHERE quantity BETWEEN 5 AND 8
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.product_id = k.product_id
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            order_id,
            product_id,
            category,
            unit_price
        FROM verified
    ) src
    PIVOT (
        AVG(unit_price) AS total
        FOR category IN ('Electronics', 'Books', 'Fashion')
    )
)
SELECT
    s.order_id,
    s.product_id,
    s.Electronics,
    s.Books,
    s.Fashion,
    (SELECT MIN(t2.quantity) FROM order_items t2 WHERE t2.product_id = s.product_id) AS min_order_item_quantity
FROM wide s
ORDER BY Fashion, min_order_item_quantity DESC;
