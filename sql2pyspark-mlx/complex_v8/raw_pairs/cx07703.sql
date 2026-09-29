WITH valid_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        YEAR(release_date) AS release_date_year,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label
    FROM tracks
    WHERE release_date >= DATE '2024-10-23'
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date_year,
        s.release_date_label,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.is_skipped AS play_is_skipped,
        p.device AS play_device,
        p.listener_id AS play_listener_id
    FROM valid_tracks s
    LEFT JOIN plays p ON p.track_id = s.id
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date_year,
        s.release_date_label,
        s.play_id,
        s.play_is_skipped,
        s.play_device,
        s.play_listener_id,
        a.is_verified AS artist_is_verified
    FROM with_children_plays s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.is_verified = TRUE
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        title,
        release_date_year,
        play_id,
        play_device,
        play_listener_id,
        artist_is_verified,
        release_date_year - LAG(release_date_year) OVER w_all_by_release_date_year AS release_date_year_change,
        SIZE(COLLECT_SET(play_device) OVER w_play_is_skipped_play_listene) AS distinct_play_device_in_play_is_skipped_play_lis,
        MAX(release_date_year) OVER w_play_is_skipped_play_listene AS play_is_skipped_play_listener_id_max_release_dat,
        ROUND(release_date_year - AVG(release_date_year) OVER (PARTITION BY play_is_skipped, play_listener_id), 2) AS release_date_year_vs_play_is_skipped_play_listen
    FROM joined
    WINDOW w_all_by_release_date_year AS (ORDER BY release_date_year ASC NULLS LAST, id, play_id), w_play_is_skipped_play_listene AS (PARTITION BY play_is_skipped, play_listener_id)
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.play_id,
        s.play_device,
        s.play_listener_id,
        s.release_date_year_change,
        s.release_date_year_vs_play_is_skipped_play_listen,
        p.id AS play_id_2,
        p.ms_played AS play_ms_played,
        p.device AS play_device_2
    FROM with_trends s
    LEFT JOIN plays p ON p.track_id = s.id
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(play_ms_played, 0) AS play_ms_played_filled,
        CASE WHEN release_date_year_change >= 405 THEN 'A' WHEN release_date_year_change >= -810 THEN 'B' ELSE 'C' END AS release_date_year_change_tier,
        CONCAT_WS('|', play_device, play_device_2) AS play_device_play_device_2_label
    FROM detail_rows_plays
),
with_trends_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        play_id,
        play_listener_id,
        release_date_year_change,
        release_date_year_vs_play_is_skipped_play_listen,
        play_id_2,
        play_device_2,
        play_ms_played_filled,
        release_date_year_change_tier,
        play_device_play_device_2_label,
        MAX(play_ms_played_filled) OVER (PARTITION BY play_device_2) AS play_device_2_max_play_ms_played_filled,
        ROW_NUMBER() OVER w_all_by_play_ms_played_filled AS rn,
        SUM(release_date_year_change) OVER (PARTITION BY play_id_2 ORDER BY release_date_year_change, id, play_id, play_id_2 ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS release_date_year_change_sum_6_prior,
        DENSE_RANK() OVER w_play_id_2_by_release_date_ye_2 AS release_date_year_change_dense_rank
    FROM derived
    WINDOW w_all_by_play_ms_played_filled AS (ORDER BY play_ms_played_filled DESC, id, play_id, play_id_2), w_play_id_2_by_release_date_ye_2 AS (PARTITION BY play_id_2 ORDER BY release_date_year_change, id, play_id, play_id_2)
)
SELECT
    id,
    play_id,
    release_date_year_change,
    release_date_year_vs_play_is_skipped_play_listen,
    play_id_2,
    play_device_2,
    play_ms_played_filled,
    release_date_year_change_tier,
    play_device_play_device_2_label,
    play_device_2_max_play_ms_played_filled,
    release_date_year_change_sum_6_prior,
    release_date_year_change_dense_rank,
    LOWER(REGEXP_REPLACE(TRIM(play_device_2), ' ', '_')) AS play_device_2_slug,
    LENGTH(TRIM(play_device_2)) AS play_device_2_len
FROM with_trends_2
