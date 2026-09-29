WITH valid_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE ms_played > 120663
),
with_parent AS (
    -- Attach listener attributes
    SELECT s.*, l.username AS listener_username, l.interests AS listener_interests
    FROM valid_plays s
    JOIN listeners l ON l.id = s.listener_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        listener_interests,
        COUNT(*) OVER (PARTITION BY track_id) AS rows_in_track_id,
        LEAD(ms_played, 3) OVER (ORDER BY ms_played DESC, id) AS next3_ms_played,
        NTILE(4) OVER (PARTITION BY device, source ORDER BY ms_played, id) AS ms_played_quartile,
        LEAD(ms_played) OVER (PARTITION BY listener_id ORDER BY ms_played, id) AS next_ms_played,
        ms_played - LAG(ms_played) OVER w_listener_id_by_ms_played AS ms_played_change,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY track_id)) AS distinct_listener_id_in_track_id,
        MIN(ms_played) OVER (ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_so_far,
        LEAD(ms_played) OVER w_all_by_ms_played AS next_ms_played_2,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY track_id), 2) AS ms_played_vs_track_id_avg,
        ROUND(CUME_DIST() OVER (ORDER BY ms_played DESC), 3) AS ms_played_cume_dist,
        LEAD(ms_played, 2) OVER (PARTITION BY listener_id ORDER BY ms_played, id) AS next2_ms_played
    FROM with_parent
    WINDOW w_listener_id_by_ms_played AS (PARTITION BY listener_id ORDER BY ms_played, id), w_all_by_ms_played AS (ORDER BY ms_played DESC, id)
),
tagged AS (
    -- Derive helper columns
    SELECT *, FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM with_trends
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    source,
    rows_in_track_id,
    next3_ms_played,
    next_ms_played,
    ms_played_change,
    distinct_listener_id_in_track_id,
    min_ms_played_so_far,
    ms_played_vs_track_id_avg,
    ms_played_cume_dist,
    next2_ms_played,
    ms_played_bucket,
    next_ms_played + ms_played_cume_dist AS next_ms_played_plus_ms_played_cume_dist
FROM tagged
ORDER BY next3_ms_played DESC NULLS FIRST
