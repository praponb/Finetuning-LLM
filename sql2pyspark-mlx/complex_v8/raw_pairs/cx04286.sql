WITH base AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label
    FROM tracks
    WHERE (bpm > 85 OR artist_id IS NOT NULL)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY bpm NULLS LAST, id) AS bpm_seq,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id, is_explicit), 0), 4) AS bpm_share_of_artist_id_is_explicit,
        LAST_VALUE(release_date) OVER (ORDER BY bpm NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date
    FROM base
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.ms_played) FROM plays p WHERE p.track_id = s.id) AS max_play_ms_played,
        (SELECT MAX(p.ms_played) FROM plays p WHERE p.track_id = s.id) AS max_play_ms_played_2,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count
    FROM with_windows s
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_stats s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
        )
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    release_date,
    is_explicit,
    release_date_label,
    bpm_seq,
    bpm_share_of_artist_id_is_explicit,
    max_play_ms_played,
    max_play_ms_played_2,
    play_count
FROM screened
WHERE max_play_ms_played_2 >= 123500
