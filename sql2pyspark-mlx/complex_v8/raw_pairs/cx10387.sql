WITH valid_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        ABS(duration_sec - 279) AS duration_sec_distance,
        DATEDIFF(DATE '2025-12-31', release_date) AS days_since_release_date,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix
    FROM tracks
    WHERE bpm >= 75
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.listener_id AS play_listener_id
    FROM valid_tracks s
    JOIN plays p ON p.track_id = s.id
),
joined_listener AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.artist_id,
        s.is_explicit,
        s.bpm,
        s.play_id,
        s.play_listener_id,
        l.country AS listener_country,
        l.username AS listener_username,
        l.plan AS listener_plan,
        p.played_at AS play_played_at_2,
        p.device AS play_device
    FROM detail_rows_plays s
    LEFT JOIN listeners l ON l.id = s.play_listener_id
    LEFT JOIN plays p ON p.id = s.play_id
),
per_group AS (
    -- Group-level totals
    SELECT
        play_device,
        artist_id,
        DATE_TRUNC('MONTH', play_played_at_2) AS play_played_at_2_month,
        ARRAY_SORT(COLLECT_SET(listener_country)) AS listener_country_list,
        SUM(bpm) AS total_bpm
    FROM joined_listener
    GROUP BY play_device, artist_id, DATE_TRUNC('MONTH', play_played_at_2)
    HAVING AVG(bpm) > 89
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id ORDER BY total_bpm DESC NULLS FIRST), 3) AS total_bpm_cume_dist,
        LAST_VALUE(total_bpm) IGNORE NULLS OVER (PARTITION BY artist_id ORDER BY total_bpm DESC NULLS FIRST, play_device, artist_id, play_played_at_2_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_total_bpm,
        DENSE_RANK() OVER (PARTITION BY play_device, artist_id ORDER BY total_bpm DESC) AS total_bpm_dense_rank,
        ROUND(AVG(total_bpm) OVER (PARTITION BY artist_id ORDER BY total_bpm, play_device, artist_id, play_played_at_2_month ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS total_bpm_avg_2_prior,
        SUM(total_bpm) OVER (PARTITION BY artist_id, play_device) AS artist_id_play_device_sum_total_bpm,
        RANK() OVER w_artist_id_by_total_bpm_ties_2 AS total_bpm_rank
    FROM per_group
    WINDOW w_artist_id_by_total_bpm_ties_2 AS (PARTITION BY artist_id ORDER BY total_bpm)
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.play_device,
        s.artist_id,
        s.play_played_at_2_month,
        s.listener_country_list,
        s.total_bpm_cume_dist,
        s.last_known_total_bpm,
        s.total_bpm_dense_rank,
        s.total_bpm_avg_2_prior,
        s.total_bpm_rank,
        a.country AS artist_country
    FROM metrics s
    JOIN artists a ON a.id = s.artist_id
),
metrics_2 AS (
    -- Running and moving aggregates
    SELECT
        play_device,
        artist_id,
        play_played_at_2_month,
        listener_country_list,
        total_bpm_cume_dist,
        last_known_total_bpm,
        total_bpm_dense_rank,
        total_bpm_avg_2_prior,
        artist_country,
        LAST_VALUE(total_bpm_avg_2_prior) OVER (PARTITION BY total_bpm_dense_rank, artist_id ORDER BY total_bpm_dense_rank ASC, play_device, artist_id, play_played_at_2_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_total_bpm_avg_2_prior,
        ROUND(total_bpm_avg_2_prior / NULLIF(SUM(total_bpm_avg_2_prior) OVER (PARTITION BY play_device), 0), 4) AS total_bpm_avg_2_prior_share_of_play_device,
        COUNT(*) OVER (PARTITION BY play_device) AS rows_in_play_device,
        RANK() OVER (PARTITION BY play_device ORDER BY last_known_total_bpm ASC) AS last_known_total_bpm_rank,
        FIRST_VALUE(total_bpm_rank) OVER (PARTITION BY play_device ORDER BY last_known_total_bpm ASC, play_device, artist_id, play_played_at_2_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_total_bpm_rank,
        LAG(total_bpm_rank) OVER w_play_device_by_last_known_to_3 AS prev_total_bpm_rank
    FROM combined_details
    WINDOW w_play_device_by_last_known_to_3 AS (PARTITION BY play_device ORDER BY last_known_total_bpm ASC, play_device, artist_id, play_played_at_2_month)
)
SELECT
    play_device,
    artist_id,
    play_played_at_2_month,
    listener_country_list,
    total_bpm_cume_dist,
    total_bpm_dense_rank,
    total_bpm_avg_2_prior,
    artist_country,
    total_bpm_avg_2_prior_share_of_play_device,
    rows_in_play_device,
    first_total_bpm_rank,
    prev_total_bpm_rank,
    ROUND(total_bpm_cume_dist * 0.9, 2) AS total_bpm_cume_dist_adj,
    SUBSTRING(TRIM(play_device), 1, 4) AS play_device_prefix
FROM metrics_2
WHERE total_bpm_avg_2_prior BETWEEN 315.0 AND 825.0
ORDER BY total_bpm_cume_dist NULLS LAST
