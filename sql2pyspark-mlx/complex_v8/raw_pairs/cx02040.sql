WITH prepared_plays AS (
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
        ABS(ms_played - 168782) AS ms_played_distance,
        DATE_TRUNC('YEAR', played_at) AS played_at_year,
        YEAR(played_at) AS played_at_year_2
    FROM plays
    WHERE is_skipped = FALSE
      AND HOUR(played_at) BETWEEN 10 AND 14
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS max_play_ms_played,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.track_id) AS last_played_at
    FROM prepared_plays s
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        track_id,
        device,
        is_skipped,
        source,
        ms_played_distance,
        played_at_year,
        played_at_year_2,
        play_count,
        last_played_at,
        FIRST_VALUE(play_count) IGNORE NULLS OVER (PARTITION BY is_skipped, DATE_TRUNC('MONTH', last_played_at) ORDER BY max_play_ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_play_count,
        LAG(play_count) OVER (PARTITION BY play_count, track_id ORDER BY ms_played, id) AS prev_play_count,
        MAX(played_at_year_2) OVER (PARTITION BY device, listener_id) AS device_listener_id_max_played_at_year_2,
        LAST_VALUE(ms_played_distance) OVER (PARTITION BY is_skipped, DATE_TRUNC('MONTH', last_played_at) ORDER BY max_play_ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played_distance
    FROM summarized_rows
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(3) OVER (ORDER BY last_played_at ASC, id) AS last_played_at_tercile,
        ROUND(first_known_play_count / NULLIF(SUM(first_known_play_count) OVER w_play_count, 0), 4) AS first_known_play_count_share_of_play_count,
        LAG(first_known_play_count, 3) OVER w_all_by_last_played_at AS prev3_first_known_play_count
    FROM with_trends
    WINDOW w_play_count AS (PARTITION BY play_count), w_all_by_last_played_at AS (ORDER BY last_played_at ASC, id)
)
SELECT
    id,
    device,
    is_skipped,
    source,
    played_at_year,
    play_count,
    last_played_at,
    device_listener_id_max_played_at_year_2,
    last_ms_played_distance,
    first_known_play_count_share_of_play_count,
    prev3_first_known_play_count,
    CASE WHEN source IN ('search', 'radio') THEN 'group_a' WHEN source = 'playlist' THEN 'group_b' ELSE 'other' END AS source_group
FROM metrics
WHERE device_listener_id_max_played_at_year_2 >= 5671
