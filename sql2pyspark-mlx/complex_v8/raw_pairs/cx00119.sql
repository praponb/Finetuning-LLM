WITH cleaned_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        DATE_FORMAT(played_at, 'MMM yyyy') AS played_at_label,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM plays
    WHERE track_id IS NOT NULL
      AND source = 'playlist'
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, MAX(played_at) AS last_played_at
    FROM plays
    GROUP BY listener_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.last_played_at
    FROM cleaned_plays s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
annotated AS (
    -- Derive helper columns
    SELECT *, ABS(ms_played - 183484) AS ms_played_distance
    FROM summarized_rows
)
-- Subtotals with CUBE
SELECT
    COALESCE(source, 'ALL') AS source,
    listener_id,
    track_id,
    COUNT(*) AS row_count,
    ARRAY_SORT(COLLECT_SET(source)) AS source_list
FROM annotated
GROUP BY CUBE(source, listener_id, track_id)
