WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        DATE_TRUNC('MONTH', release_date) AS release_date_month
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'party')
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(bpm) OVER w_is_explicit AS is_explicit_sum_bpm,
        SUM(duration_sec) OVER (PARTITION BY is_explicit ORDER BY duration_sec_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        LAG(release_date_month) OVER (PARTITION BY is_explicit ORDER BY duration_sec_filled, id) AS prev_release_date_month,
        ROUND(AVG(duration_sec_filled) OVER (PARTITION BY is_explicit ORDER BY bpm, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS duration_sec_filled_avg_4
    FROM prepared
    WINDOW w_is_explicit AS (PARTITION BY is_explicit)
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        s.duration_sec_filled,
        s.release_date_month,
        s.running_duration_sec,
        s.prev_release_date_month,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.ms_played AS play_ms_played,
        p.device AS play_device,
        p.listener_id AS play_listener_id
    FROM windowed s
    JOIN plays p ON p.track_id = s.id
),
enriched AS (
    -- Join plays
    SELECT
        s.id,
        s.artist_id,
        s.is_explicit,
        s.release_date_month,
        s.running_duration_sec,
        s.prev_release_date_month,
        s.play_id,
        s.play_played_at,
        s.play_ms_played,
        s.play_listener_id,
        p.ms_played AS play_ms_played_2,
        p.track_id AS play_track_id,
        p.source AS play_source,
        a.artist_name
    FROM line_level s
    LEFT JOIN plays p ON p.id = s.play_id
    LEFT JOIN artists a ON a.id = s.artist_id
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.is_explicit,
        s.running_duration_sec,
        s.prev_release_date_month,
        s.play_id,
        s.play_played_at,
        s.play_ms_played,
        s.play_listener_id,
        s.play_ms_played_2,
        s.play_track_id,
        s.artist_name,
        t.release_date AS track_release_date,
        t.is_explicit AS track_is_explicit
    FROM enriched s
    LEFT JOIN tracks t ON t.id = s.play_track_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        is_explicit,
        prev_release_date_month,
        play_id,
        play_ms_played,
        play_listener_id,
        artist_name,
        track_release_date,
        track_is_explicit,
        ROUND(AVG(play_ms_played_2) OVER (ORDER BY track_release_date ASC, id, play_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS play_ms_played_2_avg_7,
        LEAD(running_duration_sec) OVER w_all_by_track_release_date AS next_running_duration_sec,
        play_ms_played - LAG(play_ms_played) OVER (ORDER BY track_release_date ASC, id, play_id) AS play_ms_played_change,
        MIN(play_ms_played) OVER (ORDER BY track_release_date ASC, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_play_ms_played_so_far,
        ROW_NUMBER() OVER (ORDER BY track_release_date ASC, id, play_id) AS seq_no,
        SUM(running_duration_sec) OVER (ORDER BY track_release_date ASC, id, play_id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS running_duration_sec_sum_1_prior
    FROM combined_details
    WINDOW w_all_by_track_release_date AS (ORDER BY track_release_date ASC, id, play_id)
),
stats AS (
    -- Summarise per group
    SELECT
        CASE WHEN play_ms_played >= 239955 THEN 'large' WHEN play_ms_played >= 129336 THEN 'medium' ELSE 'small' END AS play_ms_played_tier,
        is_explicit,
        artist_id,
        SUM(running_duration_sec_sum_1_prior) AS total_running_duration_sec_sum_1_prior,
        SUM(play_ms_played_change) AS total_play_ms_played_change,
        MAX(next_running_duration_sec) AS max_next_running_duration_sec,
        DATEDIFF(MAX(track_release_date), MIN(track_release_date)) AS track_release_date_span_days,
        COUNT(*) FILTER (WHERE NOT track_is_explicit) AS n_not_track_is_explicit
    FROM with_windows
    GROUP BY CASE WHEN play_ms_played >= 239955 THEN 'large' WHEN play_ms_played >= 129336 THEN 'medium' ELSE 'small' END, is_explicit, artist_id
)
SELECT play_ms_played_tier, is_explicit, field, amount
FROM (
    SELECT play_ms_played_tier, is_explicit, track_release_date_span_days, total_running_duration_sec_sum_1_prior, n_not_track_is_explicit
    FROM stats
) src
UNPIVOT (
    amount FOR field IN (track_release_date_span_days AS m_track_release_date_span_days, total_running_duration_sec_sum_1_prior AS m_total_running_duration_sec_sum_1_prior, n_not_track_is_explicit AS n_not_track_is_explicit_v)
) unp
ORDER BY play_ms_played_tier, is_explicit, field
