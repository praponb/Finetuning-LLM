WITH recent AS (
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
        CONCAT(title, '#', CAST(bpm AS STRING)) AS title_ref,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag
    FROM tracks
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.duration_sec_filled,
        s.title_ref,
        s.is_explicit_flag,
        p.id AS play_id,
        p.source AS play_source,
        p.device AS play_device,
        p.listener_id AS play_listener_id
    FROM recent s
    JOIN plays p ON p.track_id = s.id AND p.device IN ('phone', 'desktop')
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(duration_sec_filled) OVER w_release_date_month, 2) AS release_date_month_avg_duration_sec_filled,
        ROUND(CUME_DIST() OVER w_play_listener_id_by_is_expli, 3) AS is_explicit_flag_cume_dist,
        NTILE(3) OVER w_play_listener_id_by_is_expli_2 AS is_explicit_flag_tercile,
        FIRST_VALUE(duration_sec_filled) OVER (PARTITION BY play_listener_id ORDER BY is_explicit_flag, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec_filled,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date)), 2) AS bpm_vs_release_date_month_avg
    FROM exploded_detail
    WINDOW w_release_date_month AS (PARTITION BY DATE_TRUNC('MONTH', release_date)), w_play_listener_id_by_is_expli AS (PARTITION BY play_listener_id ORDER BY is_explicit_flag), w_play_listener_id_by_is_expli_2 AS (PARTITION BY play_listener_id ORDER BY is_explicit_flag, id, play_id)
),
play_source_stats AS (
    -- Median bpm_vs_release_date_month_avg per play_source
    SELECT play_source, PERCENTILE_APPROX(bpm_vs_release_date_month_avg, 0.5) AS median_bpm_vs_release_date_month_avg
    FROM with_windows
    GROUP BY play_source
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_bpm_vs_release_date_month_avg,
        ROUND(s.bpm_vs_release_date_month_avg - g.median_bpm_vs_release_date_month_avg, 2) AS bpm_vs_release_date_month_avg_minus_median
    FROM with_windows s
    JOIN play_source_stats g ON g.play_source = s.play_source
    WHERE s.bpm_vs_release_date_month_avg >= g.median_bpm_vs_release_date_month_avg
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.title_ref,
        s.is_explicit_flag,
        s.play_id,
        s.play_listener_id,
        s.release_date_month_avg_duration_sec_filled,
        s.is_explicit_flag_tercile,
        s.first_duration_sec_filled,
        s.bpm_vs_release_date_month_avg,
        s.median_bpm_vs_release_date_month_avg,
        s.bpm_vs_release_date_month_avg_minus_median,
        p.id AS play_id_2,
        p.listener_id AS play_listener_id_2,
        p.source AS play_source_2
    FROM vs_group s
    JOIN plays p ON p.track_id = s.id
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_children
    WHERE release_date_month_avg_duration_sec_filled > (SELECT AVG(release_date_month_avg_duration_sec_filled) FROM with_children)
)
SELECT
    id,
    artist_id,
    title,
    release_date,
    title_ref,
    play_id,
    play_listener_id,
    release_date_month_avg_duration_sec_filled,
    is_explicit_flag_tercile,
    first_duration_sec_filled,
    bpm_vs_release_date_month_avg,
    median_bpm_vs_release_date_month_avg,
    bpm_vs_release_date_month_avg_minus_median,
    play_id_2,
    play_listener_id_2,
    play_source_2,
    LEAST(duration_sec, is_explicit_flag) AS least_duration_sec_is_explicit_flag,
    CONCAT_WS('-', play_source_2, title_ref) AS play_source_2_title_ref_label
FROM filtered
WHERE is_explicit_flag >= 0
ORDER BY play_source_2, play_source_2_title_ref_label DESC
