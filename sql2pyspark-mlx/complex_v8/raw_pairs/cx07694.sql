WITH valid AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        CASE WHEN product IN ('LIFE', 'HEALTH') THEN 'group_a' WHEN product = 'AUTO' THEN 'group_b' ELSE 'other' END AS product_group
    FROM policies
    WHERE end_date < DATE '2026-09-23'
      AND premium > 1042.8
),
joined AS (
    -- Enrich with dimension data
    SELECT s.*, p.joined_on AS holder_joined_on
    FROM valid s
    JOIN policyholders p ON p.id = s.holder_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(10) OVER w_start_date_year_by_holder_jo AS holder_joined_on_decile,
        ROUND(AVG(premium) OVER w_end_date_month, 2) AS end_date_month_avg_premium,
        DENSE_RANK() OVER (PARTITION BY product_group, status ORDER BY premium) AS premium_dense_rank,
        SUM(premium) OVER (PARTITION BY product_group, status ORDER BY premium, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS premium_sum_4,
        SUM(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date)) AS end_date_month_sum_premium,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date)) AS rows_in_end_date_month,
        MIN(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date)) AS end_date_month_min_premium,
        RANK() OVER (PARTITION BY product_group, status ORDER BY premium, id) AS premium_rank,
        ROUND(premium - AVG(premium) OVER w_end_date_month, 2) AS premium_vs_end_date_month_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id, product_group ORDER BY holder_joined_on), 3) AS holder_joined_on_percent_rank,
        SIZE(COLLECT_SET(status) OVER w_end_date_month) AS distinct_status_in_end_date_month
    FROM joined
    WINDOW w_start_date_year_by_holder_jo AS (PARTITION BY YEAR(start_date) ORDER BY holder_joined_on, id), w_end_date_month AS (PARTITION BY DATE_TRUNC('MONTH', end_date))
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        ABS(premium_dense_rank - 6) AS premium_dense_rank_distance,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix,
        LEAST(premium_rank, holder_joined_on_decile) AS least_premium_rank_holder_joined_on_decile
    FROM ranked
),
stats AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_ym,
        DATE_TRUNC('MONTH', holder_joined_on) AS holder_joined_on_month,
        holder_joined_on_decile,
        ROUND(SUM(least_premium_rank_holder_joined_on_decile) / NULLIF(SUM(holder_joined_on_percent_rank), 0), 3) AS least_premium_rank_holder_joined_on_decile_to_ho,
        SUM(premium_dense_rank) AS total_premium_dense_rank
    FROM annotated
    GROUP BY 1, 2, 3
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY holder_joined_on_decile, TO_DATE(holder_joined_on_month) ORDER BY total_premium_dense_rank DESC) AS total_premium_dense_rank_dense_rank,
        MAX(holder_joined_on_decile) OVER (PARTITION BY holder_joined_on_decile, TO_DATE(holder_joined_on_month) ORDER BY total_premium_dense_rank DESC, start_date_ym, holder_joined_on_month, holder_joined_on_decile ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_holder_joined_on_decile_so_far,
        RANK() OVER (ORDER BY total_premium_dense_rank DESC, start_date_ym, holder_joined_on_month, holder_joined_on_decile) AS total_premium_dense_rank_rank
    FROM stats
)
-- Final projection
SELECT
    start_date_ym,
    holder_joined_on_month,
    total_premium_dense_rank,
    total_premium_dense_rank_dense_rank,
    max_holder_joined_on_decile_so_far,
    CONCAT(start_date_ym, '#', CAST(total_premium_dense_rank AS STRING)) AS start_date_ym_ref
FROM windowed
ORDER BY start_date_ym DESC NULLS FIRST, total_premium_dense_rank_dense_rank
