WITH scoped_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        risk_score,
        joined_on,
        segment,
        ROUND(risk_score * 1.1, 2) AS risk_score_adj,
        CONCAT(region, '#', CAST(id AS STRING)) AS region_ref
    FROM policyholders
    WHERE joined_on < DATE '2025-02-22'
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_name,
        risk_score,
        segment,
        risk_score_adj,
        risk_score_adj - LAG(risk_score_adj) OVER w_segment_by_joined_on AS risk_score_adj_change,
        LAG(joined_on) OVER w_segment_joined_on_month_by_r AS prev_joined_on,
        ROUND(AVG(risk_score) OVER (PARTITION BY segment, MONTH(joined_on) ORDER BY risk_score ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS risk_score_avg_2,
        LAST_VALUE(risk_score_adj) IGNORE NULLS OVER (PARTITION BY YEAR(joined_on), segment ORDER BY risk_score_adj, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_risk_score_adj,
        FIRST_VALUE(joined_on) OVER (PARTITION BY segment ORDER BY joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_joined_on,
        ROUND(risk_score / NULLIF(SUM(risk_score) OVER (PARTITION BY segment), 0), 4) AS risk_score_share_of_segment,
        ROUND(risk_score - AVG(risk_score) OVER w_segment_joined_on_month, 2) AS risk_score_vs_segment_joined_on_month_avg,
        SUM(risk_score_adj) OVER (ORDER BY risk_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_risk_score_adj,
        LEAD(joined_on, 2) OVER (PARTITION BY YEAR(joined_on), segment ORDER BY risk_score_adj, id) AS next2_joined_on
    FROM scoped_holders
    WINDOW w_segment_by_joined_on AS (PARTITION BY segment ORDER BY joined_on DESC, id), w_segment_joined_on_month_by_r AS (PARTITION BY segment, MONTH(joined_on) ORDER BY risk_score ASC, id), w_segment_joined_on_month AS (PARTITION BY segment, DATE_TRUNC('MONTH', joined_on))
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        holder_name,
        segment,
        risk_score_adj,
        prev_joined_on,
        last_known_risk_score_adj,
        first_joined_on,
        risk_score_share_of_segment,
        risk_score_vs_segment_joined_on_month_avg,
        next2_joined_on,
        COALESCE(segment, 'UNKNOWN') AS segment_or_unknown
    FROM with_trends
),
grouped AS (
    -- Summarise per group
    SELECT
        YEAR(prev_joined_on) AS prev_joined_on_year,
        segment_or_unknown,
        SUM(CASE WHEN segment IN ('SME', 'RETAIL') THEN risk_score_adj ELSE 0 END) AS risk_score_adj_segment_in_sme_retail,
        SUM(risk_score_vs_segment_joined_on_month_avg) AS total_risk_score_vs_segment_joined_on_month_avg
    FROM enriched
    GROUP BY 1, 2
)
SELECT
    prev_joined_on_year,
    segment_or_unknown,
    risk_score_adj_segment_in_sme_retail,
    total_risk_score_vs_segment_joined_on_month_avg,
    ABS(prev_joined_on_year - 2025) AS prev_joined_on_year_distance
FROM grouped
ORDER BY segment_or_unknown
