WITH cleaned_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        ROUND(premium * 1.1, 2) AS premium_adj,
        CASE WHEN premium >= 3719.7 THEN 'HIGH' WHEN premium >= 2111.3 THEN 'MEDIUM' ELSE 'LOW' END AS premium_tier
    FROM policies
    WHERE holder_id <= 18
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(premium_adj - AVG(premium_adj) OVER w_holder_id, 2) AS premium_adj_vs_holder_id_avg,
        SIZE(COLLECT_SET(status) OVER w_holder_id) AS distinct_status_in_holder_id,
        SUM(premium_adj) OVER (PARTITION BY holder_id) AS holder_id_sum_premium_adj,
        COUNT(*) OVER (PARTITION BY holder_id) AS rows_in_holder_id,
        SUM(premium_adj) OVER (PARTITION BY premium_tier, holder_id ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium_adj,
        ROUND(premium_adj / NULLIF(SUM(premium_adj) OVER w_holder_id, 0), 4) AS premium_adj_share_of_holder_id,
        SIZE(COLLECT_SET(premium_tier) OVER (PARTITION BY holder_id)) AS distinct_premium_tier_in_holder_id,
        ROUND(CUME_DIST() OVER (PARTITION BY premium_tier, holder_id ORDER BY end_date ASC, id), 3) AS end_date_cume_dist,
        MAX(premium_adj) OVER w_holder_id AS holder_id_max_premium_adj,
        RANK() OVER w_premium_tier_status_by_start AS start_date_rank
    FROM cleaned_policys
    WINDOW w_holder_id AS (PARTITION BY holder_id), w_premium_tier_status_by_start AS (PARTITION BY premium_tier, status ORDER BY start_date)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', start_date) AS days_since_start_date,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        ROUND(running_premium_adj * 1.5, 2) AS running_premium_adj_adj
    FROM windowed
    WHERE holder_id IS NOT NULL
),
windowed_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(premium_adj) OVER (ORDER BY distinct_premium_tier_in_holder_id DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS premium_adj_avg_7,
        NTH_VALUE(premium_adj_vs_holder_id_avg, 2) OVER (PARTITION BY premium_tier, start_date_rank ORDER BY distinct_premium_tier_in_holder_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium_adj_vs_holder_id_avg,
        RANK() OVER (ORDER BY distinct_premium_tier_in_holder_id DESC, id) AS distinct_premium_tier_in_holder_id_rank,
        MAX(rows_in_holder_id) OVER (ORDER BY distinct_premium_tier_in_holder_id DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rows_in_holder_id_max_3,
        running_premium_adj_adj - LAG(running_premium_adj_adj) OVER (PARTITION BY premium_tier, start_date_rank ORDER BY distinct_premium_tier_in_holder_id, id) AS running_premium_adj_adj_change,
        DENSE_RANK() OVER (PARTITION BY premium_tier, start_date_rank ORDER BY distinct_premium_tier_in_holder_id) AS distinct_premium_tier_in_holder_id_dense_rank,
        ROUND(AVG(end_date_cume_dist) OVER (ORDER BY distinct_premium_tier_in_holder_id DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS end_date_cume_dist_avg_7
    FROM tagged
)
SELECT
    id,
    holder_id,
    start_date,
    status,
    premium_adj,
    premium_tier,
    premium_adj_vs_holder_id_avg,
    running_premium_adj,
    premium_adj_share_of_holder_id,
    end_date_cume_dist,
    holder_id_max_premium_adj,
    start_date_rank,
    running_premium_adj_adj,
    rows_in_holder_id_max_3,
    distinct_premium_tier_in_holder_id_dense_rank,
    end_date_cume_dist_avg_7
FROM windowed_2
ORDER BY start_date_rank DESC, running_premium_adj_adj DESC, id
LIMIT 10
