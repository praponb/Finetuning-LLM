WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        COALESCE(quantity, 0) AS quantity_filled,
        FLOOR(quantity) AS quantity_whole
    FROM inventory
    WHERE last_restock >= TO_DATE('2023-09-27')
),
joined AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.store_id,
        src.product_id,
        src.quantity_filled,
        s.opened_date,
        s.sqft
    FROM filtered src
    LEFT JOIN stores s
        ON src.store_id = s.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        opened_date,
        sqft,
        NTILE(3) OVER (PARTITION BY DATE_TRUNC('week', opened_date), product_id ORDER BY quantity_filled DESC, id) AS quantity_filled_bucket,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY store_id), 4) AS sqft_share,
        LAST_VALUE(sqft, TRUE) OVER (PARTITION BY product_id, store_id ORDER BY quantity_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sqft,
        NTILE(10) OVER (ORDER BY sqft DESC, id) AS sqft_bucket,
        LAG(quantity_filled, 2) OVER (PARTITION BY YEAR(opened_date), store_id ORDER BY quantity_filled, id) AS prev_quantity_filled
    FROM joined
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        sqft,
        quantity_filled_bucket,
        sqft_bucket,
        prev_quantity_filled,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sqft_share, id) AS seq_no,
        SUM(sqft) OVER (PARTITION BY store_id ORDER BY sqft_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft,
        SUM(sqft_share) OVER (PARTITION BY product_id ORDER BY sqft_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft_share,
        ROW_NUMBER() OVER w_ordered AS seq_no_2,
        ROUND(sqft_bucket / SUM(sqft_bucket) OVER (PARTITION BY product_id, store_id), 4) AS sqft_bucket_share
    FROM with_windows
    WINDOW w_ordered AS (PARTITION BY DATE_TRUNC('WEEK', opened_date), product_id ORDER BY opened_date DESC, id)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_filled_bucket NULLS FIRST, id) = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT product_id, id
    FROM latest
    EXCEPT
    SELECT product_id, id
    FROM inventory
    WHERE product_id = 1
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    JOIN remaining_keys k ON s.product_id = k.product_id AND s.id = k.id
)
SELECT
    s.id,
    s.product_id,
    s.sqft,
    s.quantity_filled_bucket,
    s.sqft_bucket,
    s.seq_no,
    s.running_sqft,
    s.sqft_bucket_share,
    (SELECT MAX(i2.quantity) FROM inventory i2 WHERE i2.product_id = s.product_id) AS max_inventory_quantity
FROM verified s
ORDER BY seq_no DESC
