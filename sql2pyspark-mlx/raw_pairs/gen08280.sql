WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        MIN(quantity) OVER w_part AS last7_min_quantity,
        MAX(quantity) OVER w_part AS last7_max_quantity,
        COUNT(quantity) OVER w_part AS last7_count_quantity,
        NTILE(3) OVER w_hist AS quantity_bucket
    FROM inventory
    WHERE store_id IN (2, 3, 4)
    WINDOW w_part AS (PARTITION BY store_id ORDER BY quantity ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY product_id ORDER BY quantity ASC, id ASC)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        quantity,
        last7_max_quantity,
        last7_count_quantity,
        quantity_bucket,
        LAG(quantity_bucket, 1, 0) OVER w_ordered AS prev_quantity_bucket,
        AVG(quantity) OVER w_grp AS quantity_avg_6
    FROM with_windows
    WINDOW w_ordered AS (PARTITION BY product_id ORDER BY last7_count_quantity ASC, id ASC), w_grp AS (PARTITION BY YEAR(last_restock), product_id ORDER BY last_restock ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
with_windows_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(quantity_avg_6) OVER w_recent AS last7_min_quantity_avg_6,
        SUM(quantity_avg_6) OVER w_recent AS last7_sum_quantity_avg_6,
        MAX(quantity_avg_6) OVER w_recent AS last7_max_quantity_avg_6,
        COUNT(quantity_avg_6) OVER w_recent AS last7_count_quantity_avg_6,
        SUM(quantity) OVER w_grp AS cumulative_quantity
    FROM scored
    WINDOW w_recent AS (ORDER BY prev_quantity_bucket ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_grp AS (ORDER BY quantity_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        quantity,
        last7_max_quantity,
        last7_count_quantity,
        prev_quantity_bucket,
        quantity_avg_6,
        last7_sum_quantity_avg_6,
        last7_max_quantity_avg_6,
        last7_count_quantity_avg_6,
        cumulative_quantity,
        FLOOR(last7_min_quantity_avg_6) AS last7_min_quantity_avg_6_whole
    FROM with_windows_inventory
    WHERE quantity = 94
),
with_windows_inventory_2 AS (
    -- Ranking and gap features
    SELECT
        id,
        quantity,
        last7_min_quantity_avg_6_whole,
        AVG(last7_min_quantity_avg_6_whole) OVER (ORDER BY last7_count_quantity ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS last7_min_quantity_avg_6_whole_avg_6_prior,
        SUM(last7_max_quantity) OVER (ORDER BY quantity_avg_6 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_last7_max_quantity
    FROM base
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(last7_min_quantity_avg_6_whole_avg_6_prior) OVER (ORDER BY last7_min_quantity_avg_6_whole_avg_6_prior ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_quantity_avg_6_whole_avg_6_prior_avg_6,
        NTILE(10) OVER (ORDER BY last7_min_quantity_avg_6_whole_avg_6_prior ASC, id ASC) AS last7_min_quantity_avg_6_whole_avg_6_prior_bucket
    FROM with_windows_inventory_2
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM enriched
    INTERSECT
    SELECT id
    FROM inventory
    WHERE quantity < 133
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.id = k.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN last7_min_quantity_avg_6_whole >= 76 THEN 'LARGE' ELSE 'SMALL' END AS last7_min_quantity_avg_6_whole_tier,
    CASE WHEN quantity >= 188 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    COUNT(*) AS row_count,
    PERCENTILE_APPROX(last7_min_quantity_avg_6_whole_avg_6_prior_avg_6, 0.95) AS last7_min_quantity_avg_6_whole_avg_6_prior_avg_6_p95,
    BIT_OR(quantity) AS quantity_bit_or,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN last7_min_quantity_avg_6_whole >= 76 THEN 'LARGE' ELSE 'SMALL' END ORDER BY BIT_OR(quantity) DESC) AS quantity_bit_or_dense_rank
FROM kept_rows
GROUP BY CASE WHEN last7_min_quantity_avg_6_whole >= 76 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN quantity >= 188 THEN 'LARGE' ELSE 'SMALL' END
HAVING last7_min_quantity_avg_6_whole_avg_6_prior_avg_6_p95 > 0;
