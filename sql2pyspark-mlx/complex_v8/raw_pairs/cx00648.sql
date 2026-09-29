WITH recent_holders AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        region,
        risk_score,
        joined_on,
        CONCAT(segment, '#', CAST(id AS STRING)) AS segment_ref
    FROM policyholders
    WHERE joined_on BETWEEN DATE '2024-02-22' AND DATE '2025-10-17'
      AND LENGTH(TRIM(holder_name)) >= 4
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM recent_holders
    WHERE risk_score > (SELECT AVG(risk_score) FROM recent_holders)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(segment_ref), 1, 2) AS segment_ref_prefix,
        LENGTH(TRIM(segment_ref)) AS segment_ref_len,
        CONCAT(segment_ref, '#', CAST(id AS STRING)) AS segment_ref_ref
    FROM screened
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        region,
        risk_score,
        joined_on,
        segment_ref_prefix,
        segment_ref_ref,
        SUM(segment_ref_len) OVER w_region AS region_sum_segment_ref_len,
        MAX(segment_ref_len) OVER (PARTITION BY region) AS region_max_segment_ref_len
    FROM derived
    WINDOW w_region AS (PARTITION BY region)
)
SELECT
    id,
    joined_on,
    segment_ref_ref,
    region_sum_segment_ref_len,
    region_max_segment_ref_len
FROM metrics
WHERE region <> 'East'
ORDER BY region_max_segment_ref_len DESC, id
LIMIT 5
