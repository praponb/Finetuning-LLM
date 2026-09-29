WITH valid AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        is_explicit,
        bpm,
        moods,
        QUARTER(release_date) AS release_date_quarter
    FROM tracks
    WHERE artist_id IS NOT NULL
      AND title IS NOT NULL
),
filtered AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.duration_sec <= 312
        )
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.release_date_quarter,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.device AS play_device,
        p.source AS play_source,
        p.listener_id AS play_listener_id
    FROM filtered s
    JOIN plays p ON p.track_id = s.id AND HOUR(p.played_at) BETWEEN 8 AND 12
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        release_date_quarter,
        play_id,
        play_played_at,
        play_source,
        ROUND(release_date_quarter / NULLIF(SUM(release_date_quarter) OVER (PARTITION BY play_device, release_date_quarter), 0), 4) AS release_date_quarter_share_of_play_device_releas,
        COUNT(*) OVER (ORDER BY play_played_at, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTILE(10) OVER (ORDER BY play_played_at, id, play_id) AS play_played_at_decile
    FROM with_children
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        play_id,
        play_played_at,
        running_count,
        play_played_at_decile,
        LEAD(play_played_at_decile) OVER (PARTITION BY play_played_at_decile, artist_id ORDER BY play_played_at, id, play_id) AS next_play_played_at_decile,
        MIN(play_played_at_decile) OVER w_play_played_at_decile AS play_played_at_decile_min_play_played_at_decile,
        LAG(play_played_at) OVER w_play_played_at_decile_artist AS prev_play_played_at,
        RANK() OVER (PARTITION BY play_played_at_decile, artist_id ORDER BY play_played_at) AS play_played_at_rank,
        SIZE(COLLECT_SET(play_source) OVER (PARTITION BY play_played_at_decile)) AS distinct_play_source_in_play_played_at_decile,
        ROUND(AVG(play_played_at_decile) OVER (PARTITION BY artist_id, play_source ORDER BY release_date_quarter ASC, id, play_id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS play_played_at_decile_avg_1_prior
    FROM ranked
    WINDOW w_play_played_at_decile AS (PARTITION BY play_played_at_decile), w_play_played_at_decile_artist AS (PARTITION BY play_played_at_decile, artist_id ORDER BY play_played_at, id, play_id)
)
SELECT
    id,
    running_count,
    play_played_at_decile,
    next_play_played_at_decile,
    play_played_at_decile_min_play_played_at_decile,
    prev_play_played_at,
    distinct_play_source_in_play_played_at_decile,
    play_played_at_decile_avg_1_prior
FROM with_windows
WHERE running_count > 5
ORDER BY play_played_at_decile_avg_1_prior, prev_play_played_at DESC NULLS FIRST
