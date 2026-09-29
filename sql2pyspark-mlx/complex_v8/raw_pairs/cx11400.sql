WITH base_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        status,
        CONCAT_WS(' / ', status, product) AS status_product_label,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM policies
    WHERE premium < 2945.9
      AND status NOT IN ('EXPIRED', 'LAPSED')
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY status_or_unknown, status)) AS distinct_holder_id_in_status_or_unknown_status,
        RANK() OVER (PARTITION BY status_or_unknown ORDER BY premium DESC, id) AS premium_rank,
        ROUND(premium - AVG(premium) OVER w_status_or_unknown_status, 2) AS premium_vs_status_or_unknown_status_avg,
        ROUND(CUME_DIST() OVER (ORDER BY end_date NULLS LAST), 3) AS end_date_cume_dist,
        LAST_VALUE(premium) OVER (PARTITION BY status_or_unknown ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_premium,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status, status_or_unknown ORDER BY premium DESC NULLS FIRST, id), 3) AS premium_percent_rank,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY status_or_unknown, status), 0), 4) AS premium_share_of_status_or_unknown_status,
        RANK() OVER (ORDER BY premium DESC, id) AS premium_rank_2,
        MAX(premium) OVER (ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_premium_so_far
    FROM base_policys
    WINDOW w_status_or_unknown_status AS (PARTITION BY status_or_unknown, status)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        premium,
        status_product_label,
        status_or_unknown,
        distinct_holder_id_in_status_or_unknown_status,
        premium_rank,
        end_date_cume_dist,
        last_premium,
        premium_percent_rank,
        premium_share_of_status_or_unknown_status,
        premium_rank_2,
        max_premium_so_far,
        LAST_VALUE(end_date_cume_dist) IGNORE NULLS OVER (ORDER BY premium_rank_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date_cume_dist,
        COUNT(*) OVER w_status_or_unknown_status AS rows_in_status_or_unknown_status,
        ROUND(premium_rank - AVG(premium_rank) OVER (PARTITION BY status_or_unknown, status), 2) AS premium_rank_vs_status_or_unknown_status_avg,
        ROUND(premium - AVG(premium) OVER w_status_or_unknown_status, 2) AS premium_vs_status_or_unknown_status_avg_2,
        COUNT(*) OVER (PARTITION BY premium_rank_2 ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM with_trends
    WINDOW w_status_or_unknown_status AS (PARTITION BY status_or_unknown, status)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN end_date_cume_dist >= 0.6 THEN 'HIGH' WHEN end_date_cume_dist >= 0.2 THEN 'MEDIUM' ELSE 'LOW' END AS end_date_cume_dist_tier,
        premium_rank,
        SUM(premium_share_of_status_or_unknown_status) AS total_premium_share_of_status_or_unknown_status,
        MIN(rows_in_status_or_unknown_status) AS min_rows_in_status_or_unknown_status,
        MIN(premium) AS min_premium,
        PERCENTILE_APPROX(running_count, 0.5) AS median_running_count,
        ROUND(STDDEV(premium_rank_vs_status_or_unknown_status_avg), 2) AS stddev_premium_rank_vs_status_or_unknown_status_
    FROM with_windows
    GROUP BY 1, 2
    HAVING AVG(premium_rank_2) > 4
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(min_premium - AVG(min_premium) OVER (PARTITION BY end_date_cume_dist_tier, premium_rank), 2) AS min_premium_vs_end_date_cume_dist_tier_premium_r,
        RANK() OVER (ORDER BY min_premium DESC, end_date_cume_dist_tier, premium_rank) AS min_premium_rank,
        ROUND(CUME_DIST() OVER (ORDER BY min_premium DESC, end_date_cume_dist_tier, premium_rank), 3) AS min_premium_cume_dist,
        ROUND(PERCENT_RANK() OVER (ORDER BY min_premium DESC), 3) AS min_premium_percent_rank
    FROM stats
)
-- Final projection
SELECT
    end_date_cume_dist_tier,
    min_rows_in_status_or_unknown_status,
    median_running_count,
    min_premium_vs_end_date_cume_dist_tier_premium_r,
    min_premium_rank,
    min_premium_cume_dist,
    min_premium_percent_rank,
    ROUND(min_premium * 0.9, 2) AS min_premium_adj,
    LOWER(REGEXP_REPLACE(TRIM(end_date_cume_dist_tier), ' ', '_')) AS end_date_cume_dist_tier_slug
FROM sequenced
ORDER BY min_premium_cume_dist
