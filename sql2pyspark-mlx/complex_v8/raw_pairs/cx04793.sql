WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        DATE_FORMAT(joined_on, 'yyyy-MM-dd') AS joined_on_label
    FROM policyholders
    WHERE joined_on >= DATE '2024-06-29'
      AND region IS NOT NULL
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        region,
        risk_score,
        segment,
        FIRST_VALUE(risk_score) IGNORE NULLS OVER (PARTITION BY region, segment ORDER BY risk_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_risk_score,
        ROUND(PERCENT_RANK() OVER (PARTITION BY region, segment ORDER BY risk_score DESC), 3) AS risk_score_percent_rank,
        SUM(risk_score) OVER w_joined_on_year_segment AS joined_on_year_segment_sum_risk_score,
        ROUND(risk_score / NULLIF(SUM(risk_score) OVER (PARTITION BY YEAR(joined_on), segment), 0), 4) AS risk_score_share_of_joined_on_year_segment,
        FIRST_VALUE(joined_on) IGNORE NULLS OVER (PARTITION BY region, segment ORDER BY risk_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_joined_on
    FROM filtered
    WINDOW w_joined_on_year_segment AS (PARTITION BY YEAR(joined_on), segment)
),
with_windows_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        first_known_risk_score - LAG(first_known_risk_score) OVER (ORDER BY risk_score_share_of_joined_on_year_segment, id) AS first_known_risk_score_change,
        ROUND(first_known_risk_score - AVG(first_known_risk_score) OVER (PARTITION BY segment), 2) AS first_known_risk_score_vs_segment_avg,
        MAX(first_known_risk_score) OVER (PARTITION BY segment) AS segment_max_first_known_risk_score,
        SIZE(COLLECT_SET(region) OVER (PARTITION BY segment)) AS distinct_region_in_segment
    FROM with_windows
)
SELECT
    id,
    region,
    risk_score,
    segment,
    risk_score_percent_rank,
    joined_on_year_segment_sum_risk_score,
    risk_score_share_of_joined_on_year_segment,
    first_known_joined_on,
    first_known_risk_score_change,
    first_known_risk_score_vs_segment_avg,
    segment_max_first_known_risk_score,
    ABS(risk_score_percent_rank - 0.3) AS risk_score_percent_rank_distance
FROM with_windows_2
ORDER BY first_known_risk_score_vs_segment_avg, segment
