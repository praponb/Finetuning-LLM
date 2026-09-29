WITH base_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug,
        DATE_TRUNC('HOUR', played_at) AS played_at_hour
    FROM plays
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.source,
        s.device_slug,
        s.played_at_hour,
        l.signup_date AS listener_signup_date,
        l.username AS listener_username,
        t.release_date AS track_release_date,
        t.moods AS track_moods
    FROM base_plays s
    JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        played_at,
        ms_played,
        source,
        device_slug,
        listener_signup_date,
        listener_username,
        track_release_date,
        track_moods,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY track_id)) AS distinct_listener_id_in_track_id,
        MAX(ms_played) OVER (PARTITION BY track_id) AS track_id_max_ms_played,
        NTH_VALUE(played_at, 2) OVER (ORDER BY listener_signup_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        ROUND(AVG(ms_played) OVER (PARTITION BY track_id), 2) AS track_id_avg_ms_played,
        COUNT(*) OVER (PARTITION BY source, listener_id) AS rows_in_source_listener_id,
        MAX(ms_played) OVER (PARTITION BY YEAR(listener_signup_date) ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ms_played_so_far,
        COUNT(*) OVER w_track_id AS rows_in_track_id,
        MIN(ms_played) OVER (PARTITION BY source, listener_id) AS source_listener_id_min_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY MONTH(played_at) ORDER BY listener_signup_date DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_2_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(played_at) ORDER BY listener_signup_date DESC, id), 3) AS listener_signup_date_cume_dist,
        RANK() OVER w_listener_signup_date_year_by_2 AS ms_played_rank,
        MAX(ms_played) OVER (PARTITION BY source, listener_id) AS source_listener_id_max_ms_played,
        MIN(ms_played) OVER (PARTITION BY track_id) AS track_id_min_ms_played
    FROM with_details
    WINDOW w_track_id AS (PARTITION BY track_id), w_listener_signup_date_year_by_2 AS (PARTITION BY YEAR(listener_signup_date) ORDER BY ms_played DESC)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        played_at,
        ms_played,
        device_slug,
        listener_username,
        track_release_date,
        rows_in_source_listener_id,
        max_ms_played_so_far,
        source_listener_id_min_ms_played,
        source_listener_id_max_ms_played,
        ROUND(AVG(rows_in_track_id) OVER (PARTITION BY listener_id, distinct_listener_id_in_track_id), 2) AS listener_id_distinct_listener_id_in_track_id_avg,
        SUM(max_ms_played_so_far) OVER (PARTITION BY listener_id, distinct_listener_id_in_track_id) AS listener_id_distinct_listener_id_in_track_id_sum,
        ROUND(AVG(track_id_min_ms_played) OVER w_listener_id_distinct_listene, 2) AS listener_id_distinct_listener_id_in_track_id_avg_2,
        ROUND(AVG(track_id_min_ms_played) OVER (PARTITION BY listener_id, source ORDER BY rows_in_track_id DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS track_id_min_ms_played_avg_4,
        FIRST_VALUE(track_release_date) OVER (ORDER BY rows_in_track_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_track_release_date,
        ROUND(CUME_DIST() OVER (ORDER BY rows_in_track_id), 3) AS rows_in_track_id_cume_dist,
        LAST_VALUE(track_id_avg_ms_played) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at) ORDER BY rows_in_track_id ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_track_id_avg_ms_played
    FROM windowed
    WINDOW w_listener_id_distinct_listene AS (PARTITION BY listener_id, distinct_listener_id_in_track_id)
)
SELECT
    id,
    listener_id,
    played_at,
    ms_played,
    device_slug,
    listener_username,
    track_release_date,
    rows_in_source_listener_id,
    max_ms_played_so_far,
    source_listener_id_max_ms_played,
    listener_id_distinct_listener_id_in_track_id_avg,
    track_id_min_ms_played_avg_4,
    rows_in_track_id_cume_dist,
    last_track_id_avg_ms_played,
    SUBSTRING(TRIM(device_slug), 1, 3) AS device_slug_prefix
FROM with_trends
ORDER BY track_id_min_ms_played_avg_4, max_ms_played_so_far DESC, id
LIMIT 10
