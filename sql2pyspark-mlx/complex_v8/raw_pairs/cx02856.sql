WITH base AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        CASE WHEN segment IN ('RETAIL', 'SME') THEN 'group_a' WHEN segment = 'VIP' THEN 'group_b' ELSE 'other' END AS segment_group
    FROM policyholders
    WHERE segment = 'RETAIL'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_all_by_risk_score AS risk_score_rank,
        ROUND(risk_score / NULLIF(SUM(risk_score) OVER (PARTITION BY region, segment_group), 0), 4) AS risk_score_share_of_region_segment_group,
        ROUND(risk_score - AVG(risk_score) OVER (PARTITION BY region, segment_group), 2) AS risk_score_vs_region_segment_group_avg,
        DENSE_RANK() OVER (ORDER BY risk_score) AS risk_score_dense_rank,
        SUM(risk_score) OVER (ORDER BY risk_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_risk_score
    FROM base
    WINDOW w_all_by_risk_score AS (ORDER BY risk_score, id)
),
leaders AS (
    -- Top 5 per group
    SELECT *
    FROM sequenced
    WHERE risk_score_rank <= 5
)
-- Subtotals with GROUPING SETS
SELECT
    risk_score_dense_rank,
    risk_score_rank,
    COUNT(*) AS row_count
FROM leaders
GROUP BY GROUPING SETS ((risk_score_dense_rank, risk_score_rank), (risk_score_dense_rank), ())
