WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        segment,
        ABS(risk_score - 0.6) AS risk_score_distance
    FROM policyholders
    WHERE joined_on IS NOT NULL
      AND region IN ('East', 'South')
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_name,
        joined_on,
        segment,
        risk_score_distance,
        LEAD(risk_score_distance) OVER (PARTITION BY DATE_TRUNC('MONTH', joined_on), region ORDER BY joined_on DESC, id) AS next_risk_score_distance,
        ROUND(risk_score_distance - AVG(risk_score_distance) OVER (PARTITION BY region, segment), 2) AS risk_score_distance_vs_region_segment_avg
    FROM cleaned
),
segment_stats AS (
    -- Avg risk_score_distance per segment
    SELECT segment, AVG(risk_score_distance) AS avg_risk_score_distance
    FROM sequenced
    GROUP BY segment
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_risk_score_distance,
        ROUND(s.risk_score_distance - g.avg_risk_score_distance, 2) AS risk_score_distance_minus_avg
    FROM sequenced s
    JOIN segment_stats g ON g.segment = s.segment
    WHERE s.risk_score_distance > g.avg_risk_score_distance
),
qualified AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM above_average s
    WHERE s.id IN (
            SELECT p.holder_id
            FROM policies p
            WHERE p.end_date BETWEEN DATE '2025-11-20' AND DATE '2026-09-23'
              AND p.premium > 2111.3
        )
      AND s.segment IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(avg_risk_score_distance) OVER (PARTITION BY segment ORDER BY risk_score_distance_vs_region_segment_avg, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS avg_risk_score_distance_avg_2_prior,
        ROUND(avg_risk_score_distance - AVG(avg_risk_score_distance) OVER (PARTITION BY MONTH(joined_on)), 2) AS avg_risk_score_distance_vs_joined_on_month_avg,
        SIZE(COLLECT_SET(segment) OVER (PARTITION BY MONTH(joined_on))) AS distinct_segment_in_joined_on_month,
        RANK() OVER (PARTITION BY segment ORDER BY risk_score_distance_vs_region_segment_avg) AS risk_score_distance_vs_region_segment_avg_rank
    FROM qualified
)
-- Final projection
SELECT
    id,
    risk_score_distance,
    next_risk_score_distance,
    risk_score_distance_vs_region_segment_avg,
    avg_risk_score_distance,
    risk_score_distance_minus_avg,
    avg_risk_score_distance_avg_2_prior,
    avg_risk_score_distance_vs_joined_on_month_avg,
    distinct_segment_in_joined_on_month,
    risk_score_distance_vs_region_segment_avg_rank,
    SUBSTRING(TRIM(holder_name), 1, 2) AS holder_name_prefix
FROM ranked
ORDER BY risk_score_distance DESC, risk_score_distance_vs_region_segment_avg DESC
