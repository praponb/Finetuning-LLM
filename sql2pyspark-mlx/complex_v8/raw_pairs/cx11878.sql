WITH recent AS (
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
    WHERE (track_id IS NOT NULL OR listener_id IS NOT NULL)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(ms_played) OVER (ORDER BY ms_played, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS ms_played_max_6_prior,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY track_id)) AS distinct_listener_id_in_track_id,
        ROW_NUMBER() OVER (ORDER BY ms_played, id) AS ms_played_seq
    FROM recent
),
joined AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.source,
        s.ms_played_max_6_prior,
        s.ms_played_seq,
        t.is_explicit AS track_is_explicit,
        a.artist_name,
        a.country AS artist_country
    FROM with_trends s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN artists a ON a.id = t.artist_id
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        SUM(ms_played) AS total_play_ms_played,
        MIN(played_at) AS first_played_at,
        MIN(played_at) AS first_played_at_2
    FROM plays
    GROUP BY listener_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.total_play_ms_played,
        p.first_played_at,
        p.first_played_at_2
    FROM joined s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS min_play_ms_played,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.track_id) AS last_played_at
    FROM profiled s
),
top_per_group AS (
    -- Top 2 per group
    SELECT *
    FROM with_stats
    WHERE ms_played_seq <= 2
)
SELECT
    id,
    track_id,
    played_at,
    device,
    ms_played_max_6_prior,
    ms_played_seq,
    track_is_explicit,
    artist_name,
    artist_country,
    total_play_ms_played,
    first_played_at_2,
    TO_DATE(played_at) AS played_at_date,
    FLOOR(ms_played_seq / 2) * 2 AS ms_played_seq_bucket
FROM top_per_group
WHERE ms_played_seq < 8
ORDER BY track_id DESC, artist_country
