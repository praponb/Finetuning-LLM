WITH scoped_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ABS(premium - 3532.7) AS premium_distance,
        DATE_FORMAT(end_date, 'yyyy-MM-dd') AS end_date_label,
        LAST_DAY(end_date) AS end_date_month_end
    FROM policies
    WHERE premium >= 2111.3
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(premium_distance / 1000) * 1000 AS premium_distance_bucket,
        DATE_ADD(end_date_month_end, 90) AS end_date_month_end_plus_90d,
        COALESCE(premium_distance, 0.0) AS premium_distance_filled
    FROM scoped_policys
),
enriched AS (
    -- Attach holder attributes
    SELECT s.*, p.segment AS holder_segment, p.region AS holder_region
    FROM derived s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT holder_id, COUNT(*) AS policy_count, COUNT(*) AS policy_count_2
    FROM policies
    GROUP BY holder_id
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_count, 0) AS policy_count,
        COALESCE(p.policy_count_2, 0) AS policy_count_2
    FROM enriched s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
enriched_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        s.premium_distance,
        s.end_date_label,
        s.end_date_month_end,
        s.premium_distance_filled,
        s.holder_segment,
        s.holder_region,
        s.policy_count,
        p.risk_score AS holder_risk_score,
        p.segment AS holder_segment_2
    FROM with_stats s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(end_date_month_end, 'yyyy-MM') AS end_date_month_end_ym,
        holder_id,
        COUNT(*) AS row_count,
        ROUND(SUM(holder_risk_score), 2) AS total_holder_risk_score,
        COUNT(DISTINCT holder_segment_2) AS distinct_holder_segment_2s
    FROM enriched_holder
    WHERE policy_count < 6
    GROUP BY DATE_FORMAT(end_date_month_end, 'yyyy-MM'), holder_id
    HAVING COUNT(*) >= 2
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(row_count, 2, 0) OVER (PARTITION BY row_count ORDER BY total_holder_risk_score DESC, end_date_month_end_ym, holder_id) AS next2_row_count,
        DENSE_RANK() OVER w_row_count_by_total_holder_ri AS total_holder_risk_score_dense_rank,
        ROUND(distinct_holder_segment_2s / NULLIF(SUM(distinct_holder_segment_2s) OVER (PARTITION BY distinct_holder_segment_2s), 0), 4) AS distinct_holder_segment_2s_share_of_distinct_hol
    FROM per_group
    WINDOW w_row_count_by_total_holder_ri AS (PARTITION BY row_count ORDER BY total_holder_risk_score DESC, end_date_month_end_ym, holder_id)
)
-- Final projection
SELECT
    end_date_month_end_ym,
    holder_id,
    total_holder_risk_score,
    distinct_holder_segment_2s,
    next2_row_count,
    total_holder_risk_score_dense_rank,
    LENGTH(TRIM(end_date_month_end_ym)) AS end_date_month_end_ym_len
FROM windowed
ORDER BY total_holder_risk_score DESC NULLS FIRST, next2_row_count
