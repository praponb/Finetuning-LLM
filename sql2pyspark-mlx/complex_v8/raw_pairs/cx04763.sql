WITH recent_plays AS (
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
        INITCAP(TRIM(device)) AS device_clean,
        TO_DATE(played_at) AS played_at_date,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM plays
    WHERE device = 'speaker'
),
enriched AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.played_at_date,
        s.source_or_unknown,
        t.moods AS track_moods
    FROM recent_plays s
    JOIN tracks t ON t.id = s.track_id AND t.duration_sec >= 142
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM enriched
    WHERE ms_played > (SELECT MIN(ms_played) FROM enriched)
)
SELECT
    id,
    track_id,
    played_at,
    ms_played,
    device,
    is_skipped,
    source,
    played_at_date,
    source_or_unknown,
    DATE_TRUNC('QUARTER', played_at_date) AS played_at_date_quarter,
    SUBSTRING(TRIM(device), 1, 2) AS device_prefix
FROM filtered
ORDER BY ms_played DESC
