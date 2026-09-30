WITH recent_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        CASE WHEN source IN ('search', 'radio') THEN 'group_a' WHEN source = 'album' THEN 'group_b' ELSE 'other' END AS source_group,
        played_at + INTERVAL 1 DAYS AS played_at_plus_1d
    FROM plays
    WHERE source = 'radio'
      AND device IN ('speaker', 'phone')
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        MIN(played_at) AS first_played_at,
        ROUND(AVG(ms_played), 2) AS avg_play_ms_played,
        SUM(ms_played) AS total_play_ms_played
    FROM plays
    GROUP BY track_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.first_played_at,
        p.avg_play_ms_played,
        p.total_play_ms_played
    FROM recent_plays s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
with_stats_2 AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS last_played_at,
        (SELECT MAX(p.ms_played) FROM plays p WHERE p.listener_id = s.listener_id) AS max_play_ms_played
    FROM with_stats s
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM with_stats_2 s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.track_id IS NOT NULL
        )
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.source,
        s.source_group,
        s.played_at_plus_1d,
        s.first_played_at,
        s.max_play_ms_played,
        t.bpm AS track_bpm,
        a.is_verified AS artist_is_verified
    FROM screened s
    JOIN tracks t ON t.id = s.track_id
    JOIN artists a ON a.id = t.artist_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(track_bpm) OVER w_artist_is_verified_track_id AS artist_is_verified_track_id_max_track_bpm,
        LEAD(played_at) OVER w_track_id_is_skipped_by_playe AS next_played_at,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY track_id, DATE_TRUNC('MONTH', played_at_plus_1d) ORDER BY track_bpm NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY track_id, DATE_TRUNC('MONTH', played_at_plus_1d) ORDER BY track_bpm NULLS LAST), 3) AS track_bpm_percent_rank
    FROM combined_details
    WINDOW w_artist_is_verified_track_id AS (PARTITION BY artist_is_verified, track_id), w_track_id_is_skipped_by_playe AS (PARTITION BY track_id, is_skipped ORDER BY played_at ASC, id)
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    ms_played,
    source,
    played_at_plus_1d,
    first_played_at,
    max_play_ms_played,
    track_bpm,
    artist_is_verified,
    artist_is_verified_track_id_max_track_bpm,
    next_played_at,
    track_bpm_percent_rank
FROM windowed
WHERE listener_id >= 18
ORDER BY artist_is_verified_track_id_max_track_bpm DESC
