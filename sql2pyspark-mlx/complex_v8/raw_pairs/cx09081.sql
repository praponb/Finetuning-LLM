WITH cleaned AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug,
        SUBSTRING(TRIM(source), 1, 3) AS source_prefix
    FROM plays
    WHERE track_id >= 15
      AND listener_id IS NOT NULL
),
joined AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.source,
        s.device_slug,
        t.bpm AS track_bpm,
        t.release_date AS track_release_date,
        t.artist_id AS track_artist_id,
        l.username AS listener_username,
        l.signup_date AS listener_signup_date
    FROM cleaned s
    JOIN tracks t ON t.id = s.track_id
    JOIN listeners l ON l.id = s.listener_id
    WHERE t.artist_id IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        track_bpm,
        track_release_date,
        track_artist_id,
        listener_username,
        ms_played - LAG(ms_played) OVER w_all_by_track_bpm AS ms_played_change,
        NTILE(5) OVER w_all_by_track_bpm AS track_bpm_quintile,
        MAX(track_bpm) OVER (PARTITION BY track_id, DATE_TRUNC('MONTH', track_release_date) ORDER BY track_release_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_track_bpm_so_far,
        DENSE_RANK() OVER (PARTITION BY track_id, DATE_TRUNC('MONTH', track_release_date) ORDER BY track_release_date DESC NULLS FIRST) AS track_release_date_dense_rank,
        ROUND(track_bpm - AVG(track_bpm) OVER w_source, 2) AS track_bpm_vs_source_avg,
        NTILE(3) OVER (PARTITION BY track_id, DATE_TRUNC('MONTH', track_release_date) ORDER BY track_release_date DESC NULLS FIRST, id) AS track_release_date_tercile,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_id ORDER BY listener_signup_date DESC), 3) AS listener_signup_date_cume_dist,
        SUM(ms_played) OVER (ORDER BY track_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER w_track_id_track_release_date_3)) / 60, 1) AS minutes_since_prev_played_at,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY source)) AS distinct_track_id_in_source,
        DENSE_RANK() OVER w_all_by_track_bpm AS track_bpm_dense_rank,
        MIN(ms_played) OVER (ORDER BY track_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_so_far,
        ROUND(AVG(ms_played) OVER w_source, 2) AS source_avg_ms_played,
        ROUND(AVG(track_bpm) OVER (PARTITION BY source), 2) AS source_avg_track_bpm,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY source), 2) AS ms_played_vs_source_avg
    FROM joined
    WINDOW w_all_by_track_bpm AS (ORDER BY track_bpm, id), w_source AS (PARTITION BY source), w_track_id_track_release_date_3 AS (PARTITION BY track_id, DATE_TRUNC('MONTH', track_release_date) ORDER BY track_release_date DESC NULLS FIRST, id)
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        track_bpm,
        track_artist_id,
        listener_username,
        ms_played_change,
        max_track_bpm_so_far,
        track_release_date_dense_rank,
        listener_signup_date_cume_dist,
        running_ms_played,
        minutes_since_prev_played_at,
        distinct_track_id_in_source,
        track_bpm_dense_rank,
        min_ms_played_so_far,
        source_avg_ms_played,
        source_avg_track_bpm,
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at,
        ADD_MONTHS(track_release_date, 1) AS track_release_date_plus_1m,
        DATEDIFF(DATE '2025-06-30', track_release_date) AS days_since_track_release_date
    FROM ranked
    WHERE max_track_bpm_so_far < 1626
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(min_ms_played_so_far) OVER w_track_artist_id_track_bpm_de AS track_artist_id_track_bpm_dense_rank_min_min_ms_,
        COUNT(*) OVER (PARTITION BY listener_id, track_release_date_dense_rank ORDER BY days_since_track_release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(source_avg_track_bpm - AVG(source_avg_track_bpm) OVER (PARTITION BY track_artist_id, track_bpm_dense_rank), 2) AS source_avg_track_bpm_vs_track_artist_id_track_bp,
        LAST_VALUE(distinct_track_id_in_source) OVER (PARTITION BY listener_id, track_release_date_dense_rank ORDER BY days_since_track_release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_distinct_track_id_in_source,
        MAX(source_avg_track_bpm) OVER w_track_artist_id_track_bpm_de AS track_artist_id_track_bpm_dense_rank_max_source_,
        RANK() OVER w_all_by_running_ms_played_tie AS running_ms_played_rank,
        LEAD(max_track_bpm_so_far, 1, 0) OVER (PARTITION BY track_bpm_dense_rank ORDER BY distinct_track_id_in_source, id) AS next_max_track_bpm_so_far
    FROM enriched
    WINDOW w_track_artist_id_track_bpm_de AS (PARTITION BY track_artist_id, track_bpm_dense_rank), w_all_by_running_ms_played_tie AS (ORDER BY running_ms_played ASC)
),
per_group AS (
    -- Group-level totals
    SELECT
        YEAR(track_release_date_plus_1m) AS track_release_date_plus_1m_year,
        listener_id,
        COUNT(DISTINCT id) AS distinct_ids,
        ARRAY_SORT(COLLECT_SET(listener_username)) AS listener_username_list,
        ROUND(STDDEV(track_artist_id_track_bpm_dense_rank_max_source_), 2) AS stddev_track_artist_id_track_bpm_dense_rank_max_,
        ROUND(SUM(minutes_since_prev_played_at), 2) AS total_minutes_since_prev_played_at
    FROM sequenced
    GROUP BY YEAR(track_release_date_plus_1m), listener_id
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(distinct_ids) OVER w_listener_id AS listener_id_max_distinct_ids,
        ROUND(AVG(distinct_ids) OVER (PARTITION BY listener_id), 2) AS listener_id_avg_distinct_ids
    FROM per_group
    WINDOW w_listener_id AS (PARTITION BY listener_id)
)
-- Final projection
SELECT
    track_release_date_plus_1m_year,
    distinct_ids,
    stddev_track_artist_id_track_bpm_dense_rank_max_,
    total_minutes_since_prev_played_at,
    listener_id_max_distinct_ids,
    listener_id_avg_distinct_ids,
    CASE WHEN distinct_ids >= 7 THEN 'A' WHEN distinct_ids >= 4 THEN 'B' ELSE 'C' END AS distinct_ids_tier
FROM sequenced_2
WHERE listener_id_max_distinct_ids <= 55
ORDER BY distinct_ids_tier NULLS LAST
