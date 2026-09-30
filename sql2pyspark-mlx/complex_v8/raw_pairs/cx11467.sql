WITH scoped_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM policies
    WHERE status IS NOT NULL
      AND start_date >= DATE '2024-07-19'
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status,
        s.premium_bucket,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score,
        p.segment AS holder_segment
    FROM scoped_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        end_date,
        status,
        premium_bucket,
        holder_joined_on,
        holder_risk_score,
        holder_segment,
        LAG(start_date) OVER w_status_by_end_date AS prev_start_date,
        ROW_NUMBER() OVER w_all_by_premium_bucket AS rn,
        ROUND(premium_bucket - AVG(premium_bucket) OVER (PARTITION BY holder_segment), 2) AS premium_bucket_vs_holder_segment_avg,
        MIN(premium_bucket) OVER (PARTITION BY holder_segment) AS holder_segment_min_premium_bucket,
        LAST_VALUE(start_date) OVER (ORDER BY premium_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date,
        LAG(premium_bucket, 1, 0) OVER (ORDER BY premium_bucket DESC, id) AS prev_premium_bucket,
        MIN(holder_risk_score) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_holder_risk_score_so_far,
        MAX(premium_bucket) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS premium_bucket_max_6_prior,
        LAST_VALUE(premium_bucket) OVER (ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_premium_bucket,
        LAG(holder_risk_score, 3) OVER (ORDER BY premium_bucket DESC, id) AS prev3_holder_risk_score,
        holder_risk_score - LAG(holder_risk_score) OVER (PARTITION BY status ORDER BY end_date, id) AS holder_risk_score_change,
        NTILE(10) OVER (ORDER BY start_date ASC, id) AS start_date_decile
    FROM joined
    WINDOW w_status_by_end_date AS (PARTITION BY status ORDER BY end_date, id), w_all_by_premium_bucket AS (ORDER BY premium_bucket DESC, id)
),
leaders AS (
    -- Top 5 per group
    SELECT
        id,
        end_date,
        status,
        premium_bucket,
        holder_joined_on,
        holder_risk_score,
        holder_segment,
        prev_start_date,
        premium_bucket_vs_holder_segment_avg,
        holder_segment_min_premium_bucket,
        last_start_date,
        prev_premium_bucket,
        min_holder_risk_score_so_far,
        premium_bucket_max_6_prior,
        last_premium_bucket,
        prev3_holder_risk_score,
        holder_risk_score_change,
        start_date_decile
    FROM with_windows
    WHERE rn <= 5
),
rollup_base AS (
    -- Group-level totals
    SELECT
        holder_segment,
        ROUND(SUM(prev3_holder_risk_score) / NULLIF(SUM(prev_premium_bucket), 0), 3) AS prev3_holder_risk_score_to_prev_premium_bucket_r,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        COUNT(*) AS row_count
    FROM leaders
    GROUP BY 1
),
with_windows_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(row_count) OVER (PARTITION BY row_count ORDER BY row_count DESC, holder_segment ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS row_count_sum_6_prior,
        SUM(row_count) OVER (PARTITION BY row_count ORDER BY row_count DESC, holder_segment ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS row_count_sum_3
    FROM rollup_base
)
SELECT
    holder_segment,
    prev3_holder_risk_score_to_prev_premium_bucket_r,
    status_list,
    row_count,
    row_count_sum_3,
    FLOOR(row_count_sum_3 / 5) * 5 AS row_count_sum_3_bucket,
    ABS(row_count_sum_6_prior - 18) AS row_count_sum_6_prior_distance
FROM with_windows_2
WHERE row_count_sum_6_prior <= 27
ORDER BY row_count_sum_3, holder_segment
LIMIT 5
