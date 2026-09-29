WITH prepared AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE device = 'car'
),
combined_details AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        t.duration_sec AS track_duration_sec,
        t.moods AS track_moods,
        t.artist_id AS track_artist_id,
        a.genre AS artist_genre,
        a.artist_name
    FROM prepared s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN artists a ON a.id = t.artist_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        played_at,
        track_duration_sec,
        track_moods,
        track_artist_id,
        ROW_NUMBER() OVER (PARTITION BY device, artist_genre ORDER BY track_duration_sec DESC, id) AS seq_no,
        SUM(track_duration_sec) OVER (PARTITION BY device, artist_genre ORDER BY track_duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_track_duration_sec,
        ROUND(track_duration_sec - AVG(track_duration_sec) OVER w_track_id_listener_id, 2) AS track_duration_sec_vs_track_id_listener_id_avg,
        NTILE(4) OVER (PARTITION BY track_artist_id ORDER BY track_duration_sec DESC, id) AS track_duration_sec_quartile,
        ROUND(AVG(track_duration_sec) OVER (PARTITION BY track_artist_id, device ORDER BY played_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS track_duration_sec_avg_3,
        DENSE_RANK() OVER (PARTITION BY track_artist_id, device ORDER BY played_at DESC) AS played_at_dense_rank,
        SUM(track_duration_sec) OVER (PARTITION BY track_id, listener_id) AS track_id_listener_id_sum_track_duration_sec,
        COUNT(*) OVER (PARTITION BY track_id, device) AS rows_in_track_id_device,
        FIRST_VALUE(track_duration_sec) IGNORE NULLS OVER (ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_track_duration_sec,
        ROUND(PERCENT_RANK() OVER (PARTITION BY device, artist_genre ORDER BY track_duration_sec DESC), 3) AS track_duration_sec_percent_rank,
        NTILE(4) OVER (PARTITION BY track_artist_id, device ORDER BY played_at DESC, id) AS played_at_quartile
    FROM combined_details
    WINDOW w_track_id_listener_id AS (PARTITION BY track_id, listener_id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(played_at_dense_rank) IGNORE NULLS OVER (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at_dense_rank,
        ROUND(AVG(track_duration_sec_quartile) OVER (PARTITION BY played_at_dense_rank ORDER BY seq_no, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS track_duration_sec_quartile_avg_2_prior,
        RANK() OVER (ORDER BY seq_no) AS seq_no_rank,
        LEAD(running_track_duration_sec) OVER (PARTITION BY played_at_dense_rank ORDER BY seq_no, id) AS next_running_track_duration_sec,
        MAX(running_track_duration_sec) OVER (PARTITION BY track_artist_id ORDER BY played_at_dense_rank ASC NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS running_track_duration_sec_max_7,
        LAG(track_duration_sec, 2, 0) OVER (PARTITION BY played_at_dense_rank ORDER BY seq_no, id) AS prev2_track_duration_sec,
        ROUND(CUME_DIST() OVER (PARTITION BY track_artist_id ORDER BY played_at_dense_rank ASC NULLS LAST), 3) AS played_at_dense_rank_cume_dist,
        MIN(played_at_quartile) OVER w_seq_no AS seq_no_min_played_at_quartile
    FROM windowed
    WINDOW w_seq_no AS (PARTITION BY seq_no)
)
-- Final projection
SELECT
    id,
    played_at,
    track_duration_sec,
    track_moods,
    seq_no,
    running_track_duration_sec,
    track_duration_sec_quartile,
    played_at_dense_rank,
    rows_in_track_id_device,
    first_known_track_duration_sec,
    track_duration_sec_percent_rank,
    played_at_quartile,
    first_known_played_at_dense_rank,
    track_duration_sec_quartile_avg_2_prior,
    seq_no_rank,
    running_track_duration_sec_max_7,
    prev2_track_duration_sec,
    played_at_dense_rank_cume_dist,
    DATE_TRUNC('DAY', played_at) AS played_at_day,
    TO_DATE(played_at) AS played_at_date
FROM with_windows
ORDER BY seq_no_rank DESC, id
