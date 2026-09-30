WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        COALESCE(region, 'UNKNOWN') AS region_or_unknown,
        ABS(risk_score - 0.3) AS risk_score_distance
    FROM policyholders
    WHERE segment = 'VIP'
      AND region IN ('East', 'Central', 'West')
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        region_or_unknown,
        risk_score_distance,
        SUM(risk_score_distance) OVER (ORDER BY joined_on, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS risk_score_distance_sum_3_prior,
        LAG(joined_on) OVER (PARTITION BY region, region_or_unknown ORDER BY joined_on, id) AS prev_joined_on,
        MIN(risk_score_distance) OVER (PARTITION BY region_or_unknown, region) AS region_or_unknown_region_min_risk_score_distance,
        ROUND(PERCENT_RANK() OVER (ORDER BY joined_on), 3) AS joined_on_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY region, region_or_unknown ORDER BY joined_on), 3) AS joined_on_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY region_or_unknown, region ORDER BY risk_score_distance DESC NULLS FIRST, id) AS risk_score_distance_seq,
        FIRST_VALUE(joined_on) IGNORE NULLS OVER (PARTITION BY region_or_unknown, region ORDER BY risk_score_distance DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_joined_on,
        RANK() OVER w_region_region_or_unknown_by AS joined_on_rank,
        RANK() OVER w_region_or_unknown_region_by_3 AS risk_score_distance_rank,
        DENSE_RANK() OVER (PARTITION BY region_or_unknown, region ORDER BY risk_score_distance DESC NULLS FIRST) AS risk_score_distance_dense_rank,
        ROUND(risk_score_distance / NULLIF(SUM(risk_score_distance) OVER (PARTITION BY region_or_unknown, region), 0), 4) AS risk_score_distance_share_of_region_or_unknown_r,
        NTILE(5) OVER (PARTITION BY region, region_or_unknown ORDER BY joined_on, id) AS joined_on_quintile
    FROM prepared
    WINDOW w_region_region_or_unknown_by AS (PARTITION BY region, region_or_unknown ORDER BY joined_on, id), w_region_or_unknown_region_by_3 AS (PARTITION BY region_or_unknown, region ORDER BY risk_score_distance DESC NULLS FIRST)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        ABS(risk_score_distance_rank - 5) AS risk_score_distance_rank_distance,
        DATE_FORMAT(first_known_joined_on, 'yyyy-MM-dd') AS first_known_joined_on_label
    FROM metrics
)
-- Final projection
SELECT
    id,
    holder_name,
    region,
    joined_on,
    region_or_unknown,
    risk_score_distance,
    risk_score_distance_sum_3_prior,
    prev_joined_on,
    joined_on_cume_dist,
    first_known_joined_on,
    joined_on_rank,
    risk_score_distance_dense_rank,
    risk_score_distance_share_of_region_or_unknown_r,
    joined_on_quintile,
    first_known_joined_on_label
FROM enriched
ORDER BY joined_on, id
LIMIT 20
