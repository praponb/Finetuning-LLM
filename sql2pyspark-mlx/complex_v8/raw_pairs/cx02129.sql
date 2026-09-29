WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM policyholders
    WHERE segment <> 'SME'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        region,
        risk_score,
        joined_on,
        DENSE_RANK() OVER (PARTITION BY region, segment ORDER BY joined_on DESC) AS joined_on_dense_rank,
        ROUND(risk_score / NULLIF(SUM(risk_score) OVER (PARTITION BY segment, region), 0), 4) AS risk_score_share_of_segment_region,
        SUM(risk_score) OVER (PARTITION BY segment, region) AS segment_region_sum_risk_score
    FROM cleaned
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY joined_on_dense_rank) AS rows_in_joined_on_dense_rank,
        LAG(joined_on_dense_rank) OVER (PARTITION BY region ORDER BY joined_on_dense_rank DESC, id) AS prev_joined_on_dense_rank
    FROM windowed
)
-- Final projection
SELECT
    id,
    region,
    joined_on,
    joined_on_dense_rank,
    risk_score_share_of_segment_region,
    segment_region_sum_risk_score,
    rows_in_joined_on_dense_rank,
    prev_joined_on_dense_rank,
    COALESCE(region, 'UNKNOWN') AS region_or_unknown,
    CASE WHEN region IN ('West', 'South') THEN 'group_a' WHEN region = 'North' THEN 'group_b' ELSE 'other' END AS region_group
FROM sequenced
ORDER BY joined_on_dense_rank
