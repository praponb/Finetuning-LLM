WITH prepared_holders AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        segment,
        CONCAT(region, '#', CAST(id AS STRING)) AS region_ref,
        MONTH(joined_on) AS joined_on_month
    FROM policyholders
    WHERE segment = 'RETAIL'
      AND joined_on BETWEEN DATE '2024-02-04' AND DATE '2025-10-01'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_name,
        risk_score,
        segment,
        joined_on_month,
        MIN(risk_score) OVER (PARTITION BY joined_on_month) AS joined_on_month_min_risk_score,
        RANK() OVER (PARTITION BY region ORDER BY risk_score DESC) AS risk_score_rank,
        ROUND(CUME_DIST() OVER w_region_by_risk_score_ties, 3) AS risk_score_cume_dist
    FROM prepared_holders
    WINDOW w_region_by_risk_score_ties AS (PARTITION BY region ORDER BY risk_score DESC)
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        holder_name,
        risk_score,
        segment,
        risk_score_cume_dist,
        CASE WHEN risk_score_rank >= 8 THEN 'HIGH' WHEN risk_score_rank >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS risk_score_rank_tier
    FROM with_windows
)
SELECT
    id,
    holder_name,
    segment,
    risk_score_cume_dist,
    risk_score_rank_tier,
    GREATEST(risk_score_cume_dist, risk_score) AS greatest_risk_score_cume_dist_risk_score
FROM derived
ORDER BY id, risk_score_rank_tier DESC
