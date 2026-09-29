WITH recent AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        segment,
        COALESCE(risk_score, 0.0) AS risk_score_filled
    FROM policyholders
    WHERE joined_on < DATE '2025-07-02'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_name,
        segment,
        risk_score_filled,
        DENSE_RANK() OVER (PARTITION BY region, segment ORDER BY risk_score_filled ASC) AS risk_score_filled_dense_rank,
        SUM(risk_score_filled) OVER (PARTITION BY region, segment ORDER BY risk_score_filled ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS risk_score_filled_sum_1_prior
    FROM recent
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS('|', segment, holder_name) AS segment_holder_name_label,
        CONCAT(segment, '#', CAST(id AS STRING)) AS segment_ref,
        COALESCE(risk_score_filled, 0.0) AS risk_score_filled_filled
    FROM with_windows
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(segment, 'ALL') AS segment,
    risk_score_filled_dense_rank,
    GROUPING(segment) AS is_total_segment,
    COUNT(*) AS row_count
FROM tagged
GROUP BY GROUPING SETS ((segment, risk_score_filled_dense_rank), (segment), (risk_score_filled_dense_rank), ())
ORDER BY segment NULLS LAST, risk_score_filled_dense_rank NULLS LAST
