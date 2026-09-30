WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        city,
        COUNT(sqft) OVER (PARTITION BY region, city ORDER BY sqft, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_sqft,
        SUM(sqft) OVER (PARTITION BY region, city ORDER BY sqft, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_sqft,
        MIN(sqft) OVER (PARTITION BY region, city ORDER BY sqft, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_sqft
    FROM stores
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        city,
        last4_count_sqft,
        last4_sum_sqft,
        last4_min_sqft,
        ROUND(last4_min_sqft / SUM(last4_min_sqft) OVER (PARTITION BY city), 4) AS last4_min_sqft_share,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY last4_min_sqft, id) AS row_seq
    FROM with_windows
    WHERE last4_count_sqft < 7
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.order_date BETWEEN '2024-03-01' AND '2025-02-01'
        )
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.sale_date > '2024-08-05'
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        last4_sum_sqft,
        last4_min_sqft_share,
        row_seq,
        ROUND(last4_count_sqft / SUM(last4_count_sqft) OVER (PARTITION BY city), 4) AS last4_count_sqft_share,
        COUNT(last4_min_sqft) OVER (PARTITION BY city) AS city_count_last4_min_sqft
    FROM kept
),
eligible_stores AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.store_id = 1
        )
),
with_windows_stores AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(last4_min_sqft_share) OVER w_part AS next_last4_min_sqft_share,
        LAG(last4_min_sqft_share) OVER w_part AS prev_last4_min_sqft_share,
        LAG(row_seq) OVER w_part AS prev_row_seq,
        ROW_NUMBER() OVER w_part AS row_seq_2,
        RANK() OVER (ORDER BY city_count_last4_min_sqft DESC NULLS LAST) AS city_count_last4_min_sqft_rank
    FROM eligible_stores
    WINDOW w_part AS (ORDER BY last4_sum_sqft, id)
)
-- Multi-level aggregation
SELECT
    CASE WHEN row_seq_2 >= 7 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_2_tier,
    COUNT(*) AS row_count,
    ROUND(AVG(last4_min_sqft_share), 2) AS avg_last4_min_sqft_share,
    APPROX_PERCENTILE(prev_row_seq, ARRAY(0.25, 0.5, 0.75)) AS prev_row_seq_quartiles
FROM with_windows_stores
GROUP BY 1
ORDER BY row_seq_2_tier NULLS FIRST;
