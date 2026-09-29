WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        CONCAT(segment, '#', CAST(id AS STRING)) AS segment_ref,
        CASE WHEN risk_score >= 0.6 THEN 'large' WHEN risk_score >= 0.3 THEN 'medium' ELSE 'small' END AS risk_score_tier
    FROM policyholders
    WHERE segment IS NOT NULL
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        segment_ref,
        ROUND(risk_score * 1.1, 2) AS risk_score_adj
    FROM cleaned
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(p.premium), 2) FROM policies p WHERE p.holder_id = s.id) AS avg_policy_premium
    FROM derived s
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY segment, region ORDER BY avg_policy_premium ASC NULLS LAST), 3) AS avg_policy_premium_percent_rank,
        ROUND(risk_score_adj - AVG(risk_score_adj) OVER (PARTITION BY region, segment), 2) AS risk_score_adj_vs_region_segment_avg
    FROM with_stats
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        holder_name,
        risk_score,
        joined_on,
        risk_score_adj,
        avg_policy_premium_percent_rank,
        risk_score_adj_vs_region_segment_avg,
        ROUND(avg_policy_premium / NULLIF(risk_score_adj, 0), 3) AS avg_policy_premium_per_risk_score_adj,
        DATE_ADD(joined_on, 365) AS joined_on_plus_365d
    FROM with_trends
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_name,
        risk_score,
        risk_score_adj,
        avg_policy_premium_percent_rank,
        risk_score_adj_vs_region_segment_avg,
        avg_policy_premium_per_risk_score_adj,
        joined_on_plus_365d,
        RANK() OVER (PARTITION BY YEAR(joined_on) ORDER BY risk_score_adj_vs_region_segment_avg) AS risk_score_adj_vs_region_segment_avg_rank,
        ROW_NUMBER() OVER (ORDER BY joined_on DESC NULLS FIRST, id) AS seq_no,
        LAG(joined_on_plus_365d, 2) OVER w_all_by_joined_on AS prev2_joined_on_plus_365d,
        ROUND(risk_score / NULLIF(SUM(risk_score) OVER (PARTITION BY MONTH(joined_on)), 0), 4) AS risk_score_share_of_joined_on_month,
        LEAD(joined_on_plus_365d, 2) OVER (ORDER BY joined_on DESC NULLS FIRST, id) AS next2_joined_on_plus_365d,
        ROUND(AVG(risk_score) OVER w_joined_on_month, 2) AS joined_on_month_avg_risk_score
    FROM tagged
    WINDOW w_all_by_joined_on AS (ORDER BY joined_on DESC NULLS FIRST, id), w_joined_on_month AS (PARTITION BY MONTH(joined_on))
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM ranked
    WHERE avg_policy_premium_percent_rank > (SELECT AVG(avg_policy_premium_percent_rank) FROM ranked)
)
SELECT
    id,
    holder_name,
    risk_score_adj,
    avg_policy_premium_percent_rank,
    risk_score_adj_vs_region_segment_avg,
    risk_score_share_of_joined_on_month,
    next2_joined_on_plus_365d,
    DATEDIFF(DATE '2026-01-01', joined_on_plus_365d) AS days_since_joined_on_plus_365d,
    DATE_FORMAT(joined_on_plus_365d, 'yyyy-MM-dd') AS joined_on_plus_365d_label
FROM filtered
ORDER BY risk_score_share_of_joined_on_month, joined_on_plus_365d_label
