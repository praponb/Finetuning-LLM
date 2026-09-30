WITH cleaned_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE source NOT IN ('album', 'radio')
),
unique_rows AS (
    -- Keep the earliest row per listener_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY played_at, id) AS row_num
        FROM cleaned_plays
    ) ranked
    WHERE row_num = 1
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(played_at) OVER (PARTITION BY device, DATE_TRUNC('MONTH', played_at) ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at,
        LAST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY device, DATE_TRUNC('MONTH', played_at) ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at
    FROM unique_rows
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.last_played_at,
        t.moods AS track_moods,
        t.artist_id AS track_artist_id
    FROM with_windows s
    LEFT JOIN tracks t ON t.id = s.track_id
)
-- Final projection
SELECT
    id,
    ms_played,
    is_skipped,
    last_played_at,
    track_moods,
    track_artist_id,
    DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label
FROM enriched
ORDER BY ms_played DESC, track_artist_id DESC NULLS FIRST
