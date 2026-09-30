WITH base_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source
    FROM plays
    WHERE ms_played < 245815
),
with_parent_listener AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.source,
        l.interests AS listener_interests,
        t.is_explicit AS track_is_explicit,
        t.release_date AS track_release_date
    FROM base_plays s
    LEFT JOIN listeners l ON l.id = s.listener_id
    JOIN tracks t ON t.id = s.track_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        listener_interests,
        track_is_explicit,
        LAG(track_release_date) OVER (PARTITION BY source ORDER BY played_at DESC, id) AS prev_track_release_date,
        DATEDIFF(track_release_date, LAG(track_release_date) OVER w_track_id_listener_id_by_play) AS days_since_prev_track_release_date,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY track_is_explicit), 0), 4) AS ms_played_share_of_track_is_explicit,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY listener_id)) AS distinct_source_in_listener_id,
        LEAD(ms_played, 1, 0) OVER (PARTITION BY track_id, listener_id ORDER BY played_at DESC, id) AS next_ms_played,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY track_id, YEAR(played_at)), 2) AS ms_played_vs_track_id_played_at_year_avg,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY listener_id), 2) AS ms_played_vs_listener_id_avg,
        SUM(ms_played) OVER (PARTITION BY listener_id) AS listener_id_sum_ms_played,
        NTILE(10) OVER w_track_is_explicit_track_id_b AS track_release_date_decile,
        MIN(ms_played) OVER (PARTITION BY track_id, listener_id ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_so_far
    FROM with_parent_listener
    WINDOW w_track_id_listener_id_by_play AS (PARTITION BY track_id, listener_id ORDER BY played_at DESC, id), w_track_is_explicit_track_id_b AS (PARTITION BY track_is_explicit, track_id ORDER BY track_release_date DESC NULLS FIRST, id)
)
SELECT
    id,
    listener_id,
    track_id,
    listener_interests,
    track_is_explicit,
    prev_track_release_date,
    days_since_prev_track_release_date,
    distinct_source_in_listener_id,
    next_ms_played,
    ms_played_vs_track_id_played_at_year_avg,
    track_release_date_decile
FROM metrics
WHERE min_ms_played_so_far < 3201000
ORDER BY distinct_source_in_listener_id DESC NULLS FIRST, id
LIMIT 20
