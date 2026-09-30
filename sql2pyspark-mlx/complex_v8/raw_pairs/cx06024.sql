WITH scoped_plays AS (
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
        DATE_TRUNC('DAY', played_at) AS played_at_day
    FROM plays
    WHERE listener_id >= 9
      AND NOT is_skipped
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM scoped_plays s
    WHERE s.listener_id IN (
            SELECT p.listener_id
            FROM plays p
        )
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.release_date AS track_release_date,
        t.moods AS track_moods,
        t.is_explicit AS track_is_explicit,
        a.is_verified AS artist_is_verified
    FROM without_matches s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN artists a ON a.id = t.artist_id
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, COUNT(*) FILTER (WHERE device = 'phone') AS play_device_count
    FROM plays
    GROUP BY track_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_device_count, 0) AS play_device_count
    FROM combined_details s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(played_at_day, 2) OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at_day,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY source), 2) AS ms_played_vs_source_avg,
        RANK() OVER w_all_by_played_at AS played_at_rank,
        ROUND(AVG(ms_played) OVER w_source, 2) AS source_avg_ms_played
    FROM with_stats
    WINDOW w_all_by_played_at AS (ORDER BY played_at, id), w_source AS (PARTITION BY source)
)
-- Final projection
SELECT
    id,
    listener_id,
    played_at,
    device,
    is_skipped,
    source,
    played_at_day,
    track_release_date,
    track_is_explicit,
    artist_is_verified,
    play_device_count,
    second_played_at_day,
    ms_played_vs_source_avg,
    played_at_rank,
    source_avg_ms_played
FROM with_windows
ORDER BY track_release_date, played_at_rank
