WITH valid AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        SUBSTRING(TRIM(source), 1, 4) AS source_prefix,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag
    FROM plays
    WHERE device IS NOT NULL
),
joined_track AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.source,
        s.source_prefix,
        s.is_skipped_flag,
        t.is_explicit AS track_is_explicit,
        t.title AS track_title,
        a.genre AS artist_genre
    FROM valid s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN artists a ON a.id = t.artist_id
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        listener_id,
        track_id,
        device,
        source,
        source_prefix,
        is_skipped_flag,
        track_is_explicit,
        artist_genre,
        IF(track_is_explicit, 'yes', 'no') AS track_is_explicit_label
    FROM joined_track
)
-- Subtotals with GROUPING SETS
SELECT
    track_is_explicit,
    COALESCE(source, 'ALL') AS source,
    COALESCE(track_is_explicit_label, 'ALL') AS track_is_explicit_label,
    GROUPING(track_is_explicit) AS is_total_track_is_explicit,
    COUNT(*) AS cnt,
    MAX(is_skipped_flag) AS max_is_skipped_flag
FROM tagged
GROUP BY GROUPING SETS ((track_is_explicit, source, track_is_explicit_label), (track_is_explicit), (source), ())
