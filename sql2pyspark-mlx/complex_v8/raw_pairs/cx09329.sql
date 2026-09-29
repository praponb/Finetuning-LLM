WITH scoped_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATE_TRUNC('YEAR', played_at) AS played_at_year,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM plays
    WHERE listener_id <= 7
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(source_or_unknown), 1, 3) AS source_or_unknown_prefix,
        CASE WHEN source_or_unknown IN ('radio', 'UNKNOWN') THEN 'group_a' WHEN source_or_unknown = 'search' THEN 'group_b' ELSE 'other' END AS source_or_unknown_group,
        INITCAP(TRIM(source)) AS source_clean
    FROM scoped_plays
)
-- Subtotals with GROUPING SETS
SELECT
    listener_id,
    source_or_unknown,
    device,
    COUNT(DISTINCT source) AS distinct_sources
FROM annotated
GROUP BY GROUPING SETS ((listener_id, source_or_unknown, device), (listener_id), (source_or_unknown), ())
ORDER BY listener_id NULLS LAST, source_or_unknown NULLS LAST, device NULLS LAST
