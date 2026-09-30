WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE voltage BETWEEN 216.8 AND 240.1
      AND kwh <= 106.5
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        kwh,
        voltage,
        quality,
        quality_or_unknown,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref,
        CONCAT_WS('|', quality, quality_or_unknown) AS quality_quality_or_unknown_label,
        COALESCE(quality_or_unknown, 'UNKNOWN') AS quality_or_unknown_or_unknown
    FROM filtered_readings
    WHERE quality_or_unknown IN ('EST', 'UNKNOWN', 'OK')
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        kwh,
        quality_or_unknown,
        quality_or_unknown_or_unknown,
        UPPER(TRIM(quality)) AS quality_clean
    FROM scored
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        quality_clean,
        CASE WHEN quality_or_unknown IN ('OK', 'UNKNOWN') THEN 'group_a' WHEN quality_or_unknown = 'EST' THEN 'group_b' ELSE 'other' END AS quality_or_unknown_group,
        LOWER(TRIM(quality_or_unknown)) AS quality_or_unknown_clean
    FROM tagged
),
summary AS (
    -- Summarise per group
    SELECT quality_or_unknown_group, COUNT(DISTINCT id) AS distinct_ids
    FROM labeled
    GROUP BY 1
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM summary
    WHERE distinct_ids < (SELECT MAX(distinct_ids) FROM summary)
)
SELECT quality_or_unknown_group, distinct_ids
FROM screened
ORDER BY distinct_ids DESC, quality_or_unknown_group DESC
