WITH recent AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        CASE WHEN ms_played >= 225733 THEN 'large' WHEN ms_played >= 107912 THEN 'medium' ELSE 'small' END AS ms_played_tier
    FROM plays
    WHERE (listener_id IS NOT NULL OR track_id >= 23)
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.track_id) AS last_played_at,
        (SELECT ROUND(AVG(p.ms_played), 2) FROM plays p WHERE p.track_id = s.track_id) AS avg_play_ms_played
    FROM recent s
),
listener_id_stats AS (
    -- Avg avg_play_ms_played per listener_id
    SELECT listener_id, AVG(avg_play_ms_played) AS avg_avg_play_ms_played
    FROM summarized_rows
    GROUP BY listener_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_avg_play_ms_played,
        ROUND(s.avg_play_ms_played - g.avg_avg_play_ms_played, 2) AS avg_play_ms_played_minus_avg
    FROM summarized_rows s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.avg_play_ms_played > g.avg_avg_play_ms_played
),
joined AS (
    -- Join tracks
    SELECT
        s.*,
        t.artist_id AS track_artist_id,
        t.bpm AS track_bpm,
        t.release_date AS track_release_date,
        a.is_verified AS artist_is_verified
    FROM vs_group s
    JOIN tracks t ON t.id = s.track_id
    JOIN artists a ON a.id = t.artist_id
    WHERE t.artist_id <= 10
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, value
FROM (
    SELECT id, ms_played, track_bpm
    FROM joined
) src
UNPIVOT (
    value FOR measure IN (ms_played, track_bpm)
) unp
