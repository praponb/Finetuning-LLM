WITH recent_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        moods
    FROM tracks
    WHERE release_date >= DATE '2024-06-03'
      AND bpm BETWEEN 73 AND 143
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.moods,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.ms_played AS play_ms_played
    FROM recent_tracks s
    LEFT JOIN plays p ON p.track_id = s.id AND p.ms_played BETWEEN 51883 AND 333795
),
with_activity AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM exploded_detail_plays s
    WHERE EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.play_listener_id
              AND l.birth_year <= 1990
        )
)
SELECT
    is_explicit,
    play_id,
    ROUND(100 * COUNT(CASE WHEN NOT is_explicit THEN 1 END) / COUNT(*), 1) AS pct_not_is_explicit,
    COUNT(*) AS cnt
FROM with_activity
GROUP BY ROLLUP(is_explicit, play_id)
