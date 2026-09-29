WITH filtered AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        segment,
        CONCAT(segment, '#', CAST(id AS STRING)) AS segment_ref,
        ABS(risk_score - 0.3) AS risk_score_distance
    FROM policyholders
    WHERE joined_on < DATE '2025-07-02'
      AND risk_score <= 0.6
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM filtered
    WHERE risk_score_distance > (SELECT AVG(risk_score_distance) FROM filtered)
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        holder_name,
        joined_on,
        segment,
        segment_ref,
        risk_score_distance,
        LOWER(REGEXP_REPLACE(TRIM(holder_name), ' ', '_')) AS holder_name_slug,
        COALESCE(segment, 'UNKNOWN') AS segment_or_unknown,
        ABS(risk_score_distance - 0.3) AS risk_score_distance_distance
    FROM screened
)
SELECT
    id,
    holder_name,
    joined_on,
    segment,
    risk_score_distance,
    holder_name_slug,
    segment_or_unknown,
    CONCAT_WS(': ', segment, segment_or_unknown) AS segment_segment_or_unknown_label,
    DAYOFWEEK(joined_on) AS joined_on_dayofweek
FROM derived
