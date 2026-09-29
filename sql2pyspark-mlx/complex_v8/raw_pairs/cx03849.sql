WITH base_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source
    FROM plays
),
with_details AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        l.interests AS listener_interests,
        t.title AS track_title,
        t.release_date AS track_release_date
    FROM base_plays s
    LEFT JOIN listeners l ON l.id = s.listener_id
    JOIN tracks t ON t.id = s.track_id
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        ROUND(AVG(ms_played), 2) AS avg_play_ms_played,
        MAX(ms_played) AS max_play_ms_played
    FROM plays
    GROUP BY listener_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.avg_play_ms_played,
        p.max_play_ms_played
    FROM with_details s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
joined AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.listener_interests,
        s.track_title,
        s.track_release_date,
        s.avg_play_ms_played,
        s.max_play_ms_played,
        t.artist_id AS track_artist_id,
        t.bpm AS track_bpm
    FROM with_stats s
    JOIN tracks t ON t.id = s.track_id
    WHERE t.artist_id IS NOT NULL
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        device,
        listener_interests,
        track_title,
        track_release_date,
        avg_play_ms_played,
        max_play_ms_played,
        track_artist_id,
        ROUND(AVG(max_play_ms_played) OVER w_track_id_listener_id, 2) AS track_id_listener_id_avg_max_play_ms_played,
        MAX(avg_play_ms_played) OVER (PARTITION BY track_id, listener_id) AS track_id_listener_id_max_avg_play_ms_played
    FROM joined
    WINDOW w_track_id_listener_id AS (PARTITION BY track_id, listener_id)
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM ranked
    WHERE avg_play_ms_played > (SELECT AVG(avg_play_ms_played) FROM ranked)
)
SELECT id, field, value
FROM (
    SELECT id, track_id_listener_id_max_avg_play_ms_played, track_id_listener_id_avg_max_play_ms_played, avg_play_ms_played
    FROM without_matches
) src
UNPIVOT (
    value FOR field IN (track_id_listener_id_max_avg_play_ms_played, track_id_listener_id_avg_max_play_ms_played, avg_play_ms_played)
) unp
