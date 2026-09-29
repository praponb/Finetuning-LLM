WITH recent_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at,
        DATE_TRUNC('DAY', played_at) AS played_at_day
    FROM plays
    WHERE ms_played < 299017
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        days_since_played_at,
        played_at_day,
        ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY ms_played, id) AS rn,
        ROUND(days_since_played_at - AVG(days_since_played_at) OVER w_source, 2) AS days_since_played_at_vs_source_avg,
        ROW_NUMBER() OVER (ORDER BY ms_played DESC, id) AS ms_played_seq,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY listener_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        MIN(days_since_played_at) OVER (PARTITION BY source) AS source_min_days_since_played_at
    FROM recent_plays
    WINDOW w_source AS (PARTITION BY source)
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        days_since_played_at,
        days_since_played_at_vs_source_avg,
        second_played_at,
        source_min_days_since_played_at,
        ROUND((UNIX_TIMESTAMP(second_played_at) - UNIX_TIMESTAMP(played_at)) / 3600, 1) AS hours_played_at_to_second_played_at,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(played_at_day)) / 3600, 1) AS hours_played_at_day_to_played_at,
        DATE_FORMAT(played_at_day, 'yyyy-MM-dd HH:00') AS played_at_day_label
    FROM metrics
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(days_since_played_at - AVG(days_since_played_at) OVER w_track_id, 2) AS days_since_played_at_vs_track_id_avg,
        RANK() OVER w_played_at_month_by_second_pl AS second_played_at_rank,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER w_listener_id_second_played_at)) / 60, 1) AS minutes_since_prev_played_at,
        NTH_VALUE(second_played_at, 2) OVER (PARTITION BY listener_id, TO_DATE(second_played_at) ORDER BY source_min_days_since_played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_played_at,
        days_since_played_at - LAG(days_since_played_at) OVER (PARTITION BY listener_id, TO_DATE(second_played_at) ORDER BY source_min_days_since_played_at DESC, id) AS days_since_played_at_change
    FROM labeled
    WINDOW w_track_id AS (PARTITION BY track_id), w_played_at_month_by_second_pl AS (PARTITION BY DATE_TRUNC('MONTH', played_at) ORDER BY second_played_at ASC), w_listener_id_second_played_at AS (PARTITION BY listener_id, TO_DATE(second_played_at) ORDER BY source_min_days_since_played_at DESC, id)
),
combined_details_track AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.source_min_days_since_played_at,
        s.hours_played_at_to_second_played_at,
        s.hours_played_at_day_to_played_at,
        s.played_at_day_label,
        s.days_since_played_at_vs_track_id_avg,
        s.second_played_at_rank,
        s.second_second_played_at,
        s.days_since_played_at_change,
        t.moods AS track_moods,
        t.duration_sec AS track_duration_sec,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year
    FROM ranked s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN artists a ON a.id = t.artist_id
    WHERE SIZE(t.moods) >= 2
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    hours_played_at_to_second_played_at,
    played_at_day_label,
    days_since_played_at_vs_track_id_avg,
    second_played_at_rank,
    days_since_played_at_change,
    track_duration_sec,
    artist_country,
    artist_debut_year
FROM combined_details_track
ORDER BY track_id, id
LIMIT 5
