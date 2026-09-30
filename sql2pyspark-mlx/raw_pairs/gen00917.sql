WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, FLOOR(quantity) AS quantity_whole
    FROM inventory
    WHERE last_restock > DATE '2023-03-31'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY quantity, id) AS quantity_bucket,
        NTILE(3) OVER (PARTITION BY store_id ORDER BY quantity_whole DESC, id) AS quantity_whole_bucket
    FROM valid
    WHERE quantity_whole IS NOT NULL
),
valid_inventory AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        quantity,
        last_restock,
        quantity_bucket,
        quantity_whole_bucket,
        quantity_whole * quantity_whole_bucket AS quantity_whole_times_quantity_whole_bucket,
        ROUND(quantity * 100, 2) AS quantity_scaled
    FROM with_windows
    WHERE last_restock >= DATE '2023-02-13'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        quantity,
        quantity_bucket,
        quantity_whole_bucket,
        quantity_whole_times_quantity_whole_bucket,
        quantity_scaled,
        AVG(quantity) OVER (ORDER BY quantity_whole_bucket, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS quantity_avg_5_prior,
        ROW_NUMBER() OVER (ORDER BY quantity DESC, id) AS seq_no,
        FIRST_VALUE(quantity_bucket, TRUE) OVER (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity_bucket,
        NTILE(3) OVER (PARTITION BY DATE_TRUNC('week', last_restock) ORDER BY quantity_bucket, id) AS quantity_bucket_bucket,
        CUME_DIST() OVER (ORDER BY quantity_bucket DESC NULLS FIRST) AS quantity_bucket_cume_dist
    FROM valid_inventory
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        quantity,
        quantity_whole_bucket,
        quantity_whole_times_quantity_whole_bucket,
        quantity_avg_5_prior,
        first_known_quantity_bucket,
        FIRST_VALUE(first_known_quantity_bucket, TRUE) OVER (ORDER BY quantity_whole_times_quantity_whole_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_first_known_quantity_bucket,
        NTILE(10) OVER w_part AS quantity_bucket_bucket_bucket,
        SUM(quantity_bucket) OVER (ORDER BY quantity_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity_bucket,
        NTILE(5) OVER w_ordered AS quantity_avg_5_prior_bucket,
        RANK() OVER (ORDER BY quantity_bucket NULLS LAST) AS quantity_bucket_rank
    FROM enriched
    WINDOW w_part AS (ORDER BY quantity_bucket_bucket, id), w_ordered AS (ORDER BY quantity_avg_5_prior, id)
)
SELECT
    id,
    quantity,
    quantity_whole_times_quantity_whole_bucket,
    first_known_quantity_bucket,
    first_known_first_known_quantity_bucket,
    quantity_bucket_bucket_bucket,
    quantity_avg_5_prior_bucket
FROM scored;
