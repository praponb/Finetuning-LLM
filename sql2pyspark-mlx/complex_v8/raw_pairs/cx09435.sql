WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label,
        SUBSTRING(TRIM(product), 1, 2) AS product_prefix
    FROM policies
    WHERE premium < 4670.9
      AND YEAR(end_date) = 2026
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        ROUND(AVG(premium) OVER (PARTITION BY status), 2) AS status_avg_premium,
        DENSE_RANK() OVER (PARTITION BY holder_id, MONTH(end_date) ORDER BY end_date) AS end_date_dense_rank,
        ROUND(premium - AVG(premium) OVER (PARTITION BY status), 2) AS premium_vs_status_avg,
        NTILE(3) OVER w_holder_id_end_date_month_by_2 AS end_date_tercile,
        SUM(premium) OVER (PARTITION BY status ORDER BY start_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY status ORDER BY start_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY premium) AS premium_dense_rank
    FROM filtered
    WINDOW w_holder_id_end_date_month_by_2 AS (PARTITION BY holder_id, MONTH(end_date) ORDER BY end_date, id)
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM with_trends
    WHERE end_date_dense_rank = 1
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(premium_dense_rank) OVER (PARTITION BY end_date_dense_rank) AS end_date_dense_rank_sum_premium_dense_rank,
        DENSE_RANK() OVER w_end_date_dense_rank_by_rows AS rows_in_status_dense_rank,
        ROUND(end_date_dense_rank - AVG(end_date_dense_rank) OVER (PARTITION BY end_date_dense_rank), 2) AS end_date_dense_rank_vs_end_date_dense_rank_avg,
        ROUND(premium_vs_status_avg / NULLIF(SUM(premium_vs_status_avg) OVER w_end_date_dense_rank, 0), 4) AS premium_vs_status_avg_share_of_end_date_dense_ra
    FROM top_per_group
    WINDOW w_end_date_dense_rank_by_rows AS (PARTITION BY end_date_dense_rank ORDER BY rows_in_status), w_end_date_dense_rank AS (PARTITION BY end_date_dense_rank)
),
grouped AS (
    -- Summarise per group
    SELECT
        CASE WHEN rows_in_status >= 15 THEN 'large' WHEN rows_in_status >= 6 THEN 'medium' ELSE 'small' END AS rows_in_status_tier,
        end_date_dense_rank,
        SUM(premium_dense_rank) AS total_premium_dense_rank,
        COUNT(DISTINCT product) AS distinct_products
    FROM windowed
    GROUP BY 1, 2
    HAVING SUM(running_premium) > 12650.0
)
-- Final projection
SELECT
    rows_in_status_tier,
    end_date_dense_rank,
    distinct_products,
    FLOOR(end_date_dense_rank / 2) * 2 AS end_date_dense_rank_bucket,
    SUBSTRING(TRIM(rows_in_status_tier), 1, 4) AS rows_in_status_tier_prefix
FROM grouped
WHERE end_date_dense_rank < 7
ORDER BY rows_in_status_tier_prefix DESC, rows_in_status_tier
