WITH recent_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        LENGTH(TRIM(source)) AS source_len
    FROM plays
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        source,
        source_len,
        is_skipped,
        MIN(ms_played) AS min_ms_played,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids,
        ROUND(AVG(source_len), 2) AS avg_source_len,
        MAX(played_at) AS last_played_at,
        SUM(CASE WHEN is_skipped = TRUE THEN source_len ELSE 0 END) AS source_len_is_skipped_true,
        ROUND(SUM(source_len) / NULLIF(SUM(ms_played), 0), 3) AS source_len_to_ms_played_ratio
    FROM recent_plays
    GROUP BY source, source_len, is_skipped
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_group
    WHERE source_len_to_ms_played_ratio > (SELECT AVG(source_len_to_ms_played_ratio) FROM per_group)
),
enriched AS (
    -- Derive helper columns
    SELECT *, CONCAT(source, '#', CAST(min_ms_played AS STRING)) AS source_ref
    FROM without_matches
),
enriched_2 AS (
    -- Label and score rows
    SELECT *, LENGTH(TRIM(source)) AS source_len_2
    FROM enriched
),
tagged AS (
    -- Label and score rows
    SELECT *, COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM enriched_2
    WHERE min_ms_played BETWEEN 64250 AND 281500
)
-- Final projection
SELECT
    source,
    is_skipped,
    min_ms_played,
    avg_source_len,
    last_played_at,
    source_len_is_skipped_true,
    source_ref,
    source_len_2,
    source_or_unknown,
    TO_DATE(last_played_at) AS last_played_at_date
FROM tagged
ORDER BY last_played_at DESC
