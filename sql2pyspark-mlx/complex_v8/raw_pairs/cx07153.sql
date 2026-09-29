WITH recent_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATE_TRUNC('HOUR', played_at) AS played_at_hour
    FROM plays
    WHERE TO_DATE(played_at) < DATE '2025-02-18'
),
with_parent_track AS (
    -- Join tracks
    SELECT
        s.*,
        t.bpm AS track_bpm,
        t.moods AS track_moods,
        t.duration_sec AS track_duration_sec,
        a.country AS artist_country
    FROM recent_plays s
    JOIN tracks t ON t.id = s.track_id AND SIZE(t.moods) >= 1
    LEFT JOIN artists a ON a.id = t.artist_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        played_at_hour,
        track_moods,
        artist_country,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at)) AS rows_in_played_at_month,
        FIRST_VALUE(played_at_hour) IGNORE NULLS OVER (PARTITION BY device, is_skipped ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at_hour,
        ROUND(AVG(track_bpm) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at)), 2) AS played_at_month_avg_track_bpm,
        FIRST_VALUE(track_bpm) OVER (ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_track_bpm,
        LEAD(played_at_hour, 2) OVER (PARTITION BY track_id, MONTH(played_at) ORDER BY played_at, id) AS next2_played_at_hour,
        COUNT(*) OVER (PARTITION BY device, track_id) AS rows_in_device_track_id,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', played_at_hour), is_skipped ORDER BY played_at), 3) AS played_at_cume_dist,
        LAST_VALUE(played_at_hour) OVER (PARTITION BY device, is_skipped ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at_hour,
        SUM(track_duration_sec) OVER (PARTITION BY track_id, MONTH(played_at) ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_track_duration_sec
    FROM with_parent_track
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        track_id,
        played_at,
        device,
        source,
        played_at_hour,
        track_moods,
        artist_country,
        rows_in_played_at_month,
        first_known_played_at_hour,
        played_at_month_avg_track_bpm,
        first_track_bpm,
        next2_played_at_hour,
        played_at_cume_dist,
        running_track_duration_sec,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug,
        UPPER(TRIM(source)) AS source_clean,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
    FROM ranked
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        track_id,
        device,
        source,
        played_at_hour,
        track_moods,
        rows_in_played_at_month,
        first_known_played_at_hour,
        next2_played_at_hour,
        running_track_duration_sec,
        LEAD(first_track_bpm) OVER (ORDER BY played_at_cume_dist DESC, id) AS next_first_track_bpm,
        LEAD(next2_played_at_hour, 2) OVER (ORDER BY played_at_month_avg_track_bpm, id) AS next2_next2_played_at_hour,
        LAST_VALUE(next2_played_at_hour) OVER (PARTITION BY device ORDER BY first_known_played_at_hour DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_next2_played_at_hour,
        ROUND(PERCENT_RANK() OVER w_all_by_played_at_cume_dist_t, 3) AS played_at_cume_dist_percent_rank,
        FIRST_VALUE(played_at_cume_dist) OVER (ORDER BY played_at_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at_cume_dist,
        SIZE(COLLECT_SET(device) OVER w_artist_country) AS distinct_device_in_artist_country,
        RANK() OVER (PARTITION BY YEAR(next2_played_at_hour) ORDER BY first_track_bpm ASC, id) AS first_track_bpm_rank,
        ROUND(rows_in_played_at_month / NULLIF(SUM(rows_in_played_at_month) OVER (PARTITION BY artist_country), 0), 4) AS rows_in_played_at_month_share_of_artist_country
    FROM scored
    WINDOW w_all_by_played_at_cume_dist_t AS (ORDER BY played_at_cume_dist DESC), w_artist_country AS (PARTITION BY artist_country)
)
SELECT
    id,
    device,
    played_at_hour,
    track_moods,
    rows_in_played_at_month,
    first_known_played_at_hour,
    running_track_duration_sec,
    next_first_track_bpm,
    next2_next2_played_at_hour,
    last_next2_played_at_hour,
    played_at_cume_dist_percent_rank,
    distinct_device_in_artist_country,
    first_track_bpm_rank,
    rows_in_played_at_month_share_of_artist_country,
    CONCAT_WS('|', source, device) AS source_device_label
FROM windowed
