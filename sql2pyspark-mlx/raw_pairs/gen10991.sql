WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_FORMAT(last_restock, 'MMM yyyy') AS last_restock_label,
        CASE WHEN quantity >= 245 THEN 'High' WHEN quantity >= 120 THEN 'Medium' ELSE 'Low' END AS quantity_tier
    FROM inventory
    WHERE last_restock > '2024-10-01'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id ASC) AS seq_no,
        AVG(quantity) OVER (PARTITION BY store_id, product_id ORDER BY quantity ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_avg_6_prior
    FROM filtered
),
with_windows_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(quantity_avg_6_prior) OVER w_grp AS running_sum_quantity_avg_6_prior,
        MAX(quantity_avg_6_prior) OVER w_grp AS running_max_quantity_avg_6_prior,
        MIN(quantity_avg_6_prior) OVER w_grp AS running_min_quantity_avg_6_prior,
        AVG(quantity_avg_6_prior) OVER w_grp AS running_avg_quantity_avg_6_prior,
        NTILE(3) OVER w_recent AS quantity_bucket
    FROM with_windows
    WHERE seq_no <= 7
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY seq_no ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY product_id ORDER BY quantity ASC, id ASC)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        quantity,
        quantity_tier,
        running_sum_quantity_avg_6_prior,
        running_max_quantity_avg_6_prior,
        running_min_quantity_avg_6_prior,
        running_avg_quantity_avg_6_prior,
        quantity_bucket
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_bucket ASC NULLS FIRST, id ASC) AS latest_rank
        FROM with_windows_inventory
    ) ranked
    WHERE latest_rank = 1
),
with_windows_inventory_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(running_avg_quantity_avg_6_prior, 1) OVER w_ordered AS next_running_avg_quantity_avg_6_prior,
        AVG(quantity) OVER w_grp AS quantity_avg_6_prior,
        FIRST_VALUE(running_avg_quantity_avg_6_prior, TRUE) OVER (ORDER BY running_sum_quantity_avg_6_prior ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_avg_quantity_avg_6_prior,
        LAG(running_sum_quantity_avg_6_prior, 2) OVER w_ordered AS prev_running_sum_quantity_avg_6_prior,
        NTILE(4) OVER w_hist AS running_avg_quantity_avg_6_prior_bucket
    FROM latest
    WINDOW w_ordered AS (ORDER BY running_max_quantity_avg_6_prior ASC, id ASC), w_grp AS (ORDER BY quantity_bucket ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), w_hist AS (ORDER BY running_avg_quantity_avg_6_prior DESC, id ASC)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN running_max_quantity_avg_6_prior >= 239 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_max_quantity_avg_6_prior_tier,
    CASE WHEN first_known_running_avg_quantity_avg_6_prior >= 253 THEN 'HIGH' ELSE 'LOW' END AS first_known_running_avg_quantity_avg_6_prior_tier,
    PERCENTILE_APPROX(running_avg_quantity_avg_6_prior_bucket, ARRAY(0.25, 0.5, 0.75)) AS running_avg_quantity_avg_6_prior_bucket_quartiles,
    SUM(quantity) AS total_quantity,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_windows_inventory_2
GROUP BY CUBE (CASE WHEN running_max_quantity_avg_6_prior >= 239 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN first_known_running_avg_quantity_avg_6_prior >= 253 THEN 'HIGH' ELSE 'LOW' END)
HAVING SUM(quantity) > 0;
