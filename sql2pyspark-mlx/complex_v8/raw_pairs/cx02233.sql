WITH filtered AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        ABS(risk_score - 0.2) AS risk_score_distance
    FROM policyholders
    WHERE holder_name IS NOT NULL
      AND region IN ('South', 'North', 'West')
),
without_matches AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
        )
      AND s.risk_score_distance > 0.3
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        risk_score_distance,
        DENSE_RANK() OVER w_segment_by_risk_score_ties AS risk_score_dense_rank,
        MIN(risk_score_distance) OVER (PARTITION BY segment, region) AS segment_region_min_risk_score_distance,
        LAST_VALUE(joined_on) IGNORE NULLS OVER (ORDER BY risk_score_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_joined_on
    FROM without_matches
    WINDOW w_segment_by_risk_score_ties AS (PARTITION BY segment ORDER BY risk_score)
),
with_activity AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
        )
),
segment_stats AS (
    -- Max risk_score_dense_rank per segment
    SELECT segment, MAX(risk_score_dense_rank) AS max_risk_score_dense_rank
    FROM with_activity
    GROUP BY segment
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_risk_score_dense_rank,
        ROUND(s.risk_score_dense_rank - g.max_risk_score_dense_rank, 2) AS risk_score_dense_rank_minus_max
    FROM with_activity s
    JOIN segment_stats g ON g.segment = s.segment
    WHERE s.risk_score_dense_rank = g.max_risk_score_dense_rank
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        holder_name,
        region,
        segment,
        risk_score_dense_rank,
        segment_region_min_risk_score_distance,
        last_known_joined_on,
        risk_score_dense_rank_minus_max,
        FLOOR(risk_score_dense_rank / 2) * 2 AS risk_score_dense_rank_bucket
    FROM compared
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (ORDER BY last_known_joined_on NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        risk_score_dense_rank - LAG(risk_score_dense_rank) OVER (ORDER BY last_known_joined_on NULLS LAST, id) AS risk_score_dense_rank_change,
        COUNT(*) OVER (PARTITION BY segment, region) AS rows_in_segment_region
    FROM annotated
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, reading
FROM (
    SELECT id, risk_score_dense_rank_bucket, risk_score_dense_rank_change, running_count, rows_in_segment_region
    FROM windowed
) src
UNPIVOT (
    reading FOR metric IN (risk_score_dense_rank_bucket, risk_score_dense_rank_change, running_count, rows_in_segment_region)
) unp
