WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        LAG(quantity, 1) OVER (ORDER BY quantity, id) AS prev_quantity,
        FIRST_VALUE(quantity) IGNORE NULLS OVER (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity
    FROM inventory
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC NULLS FIRST, id) AS latest_rank
        FROM enriched
    ) t
    WHERE latest_rank = 1
),
enriched_inventory AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        quantity,
        prev_quantity,
        first_known_quantity,
        NTILE(4) OVER (ORDER BY prev_quantity DESC, id) AS prev_quantity_bucket,
        NTILE(3) OVER (PARTITION BY product_id ORDER BY first_known_quantity, id) AS first_known_quantity_bucket,
        NTILE(3) OVER w_grp AS prev_quantity_bucket_2
    FROM latest
    WINDOW w_grp AS (ORDER BY prev_quantity, id)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        first_known_quantity,
        prev_quantity_bucket,
        first_known_quantity_bucket,
        prev_quantity_bucket_2,
        MAX(prev_quantity) OVER (PARTITION BY product_id) AS product_id_max_prev_quantity,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY prev_quantity_bucket_2, id) AS prev_quantity_bucket_2_bucket,
        LAG(first_known_quantity_bucket, 1) OVER (PARTITION BY product_id ORDER BY first_known_quantity, id) AS prev_first_known_quantity_bucket,
        LAG(quantity, 1) OVER (PARTITION BY product_id ORDER BY prev_quantity, id) AS prev_quantity_2,
        LAG(prev_quantity_bucket, 2) OVER w_part AS prev_prev_quantity_bucket
    FROM enriched_inventory
    WHERE prev_quantity != 228
    WINDOW w_part AS (PARTITION BY product_id ORDER BY first_known_quantity_bucket, id)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        first_known_quantity,
        prev_quantity_bucket,
        prev_quantity_bucket_2_bucket,
        prev_first_known_quantity_bucket,
        prev_quantity_2,
        prev_prev_quantity_bucket,
        prev_quantity_bucket + prev_first_known_quantity_bucket AS prev_quantity_bucket_plus_prev_first_known_quantity_bucket
    FROM with_windows
    WHERE first_known_quantity BETWEEN 76 AND 229
),
enriched_inventory_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(prev_first_known_quantity_bucket) OVER w_recent AS cumulative_prev_first_known_quantity_bucket,
        SUM(prev_first_known_quantity_bucket) OVER w_part AS running_prev_first_known_quantity_bucket,
        NTILE(10) OVER (ORDER BY prev_quantity_2 DESC, id) AS prev_quantity_2_bucket
    FROM base
    WINDOW w_recent AS (ORDER BY prev_prev_quantity_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (ORDER BY prev_first_known_quantity_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
summary AS (
    -- Grouped metrics
    SELECT
        CASE WHEN prev_prev_quantity_bucket >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_prev_quantity_bucket_tier,
        CASE WHEN cumulative_prev_first_known_quantity_bucket >= 41 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_prev_first_known_quantity_bucket_tier,
        ROUND(AVG(running_prev_first_known_quantity_bucket), 2) AS avg_running_prev_first_known_quantity_bucket,
        COUNT(*) AS event_count,
        SUM(cumulative_prev_first_known_quantity_bucket) AS total_cumulative_prev_first_known_quantity_bucket,
        SUM(prev_prev_quantity_bucket) AS total_prev_prev_quantity_bucket,
        SUM(prev_first_known_quantity_bucket) AS total_prev_first_known_quantity_bucket
    FROM enriched_inventory_2
    GROUP BY CASE WHEN prev_prev_quantity_bucket >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN cumulative_prev_first_known_quantity_bucket >= 41 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    prev_prev_quantity_bucket_tier,
    cumulative_prev_first_known_quantity_bucket_tier,
    avg_running_prev_first_known_quantity_bucket,
    event_count,
    total_prev_prev_quantity_bucket
FROM summary;
