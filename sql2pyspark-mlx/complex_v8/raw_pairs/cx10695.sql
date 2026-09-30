WITH scoped AS (
    -- Filter plays
    SELECT *
    FROM plays
    WHERE track_id IS NOT NULL
      AND ms_played BETWEEN 42041 AND 225733
),
with_details_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        l.username AS listener_username,
        t.title AS track_title
    FROM scoped s
    JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
    WHERE UPPER(TRIM(l.username)) LIKE 'D%'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        track_title,
        MIN(ms_played) OVER (PARTITION BY listener_id) AS listener_id_min_ms_played,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY listener_id), 2) AS ms_played_vs_listener_id_avg,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY track_id, device ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        LAG(played_at) OVER (PARTITION BY is_skipped, track_id ORDER BY played_at DESC, id) AS prev_played_at,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY listener_id, source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at_2,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY listener_id)) AS distinct_track_id_in_listener_id,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY TO_DATE(played_at)), 0), 4) AS ms_played_share_of_played_at_to_date,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY TO_DATE(played_at)), 2) AS ms_played_vs_played_at_to_date_avg,
        ROUND(AVG(ms_played) OVER (PARTITION BY is_skipped ORDER BY ms_played, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_1_prior,
        COUNT(*) OVER w_listener_id AS rows_in_listener_id,
        DENSE_RANK() OVER w_listener_id_source_by_played_2 AS played_at_dense_rank
    FROM with_details_listener
    WINDOW w_listener_id AS (PARTITION BY listener_id), w_listener_id_source_by_played_2 AS (PARTITION BY listener_id, source ORDER BY played_at)
),
first_rows AS (
    -- Top 2 per group
    SELECT *
    FROM with_windows
    WHERE played_at_dense_rank <= 2
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY device, distinct_track_id_in_listener_id)) AS distinct_track_id_in_device_distinct_track_id_in,
        NTILE(10) OVER (PARTITION BY device ORDER BY second_played_at_2 ASC, id) AS second_played_at_2_decile,
        MAX(rows_in_listener_id) OVER (ORDER BY prev_played_at DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS rows_in_listener_id_max_3_prior,
        DENSE_RANK() OVER (PARTITION BY device ORDER BY second_played_at_2 ASC, id) AS second_played_at_2_dense_rank,
        DENSE_RANK() OVER (ORDER BY prev_played_at DESC) AS prev_played_at_dense_rank,
        NTILE(4) OVER (PARTITION BY device ORDER BY second_played_at_2 ASC, id) AS second_played_at_2_quartile,
        SUM(ms_played_avg_1_prior) OVER (PARTITION BY source, device ORDER BY ms_played_vs_played_at_to_date_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played_avg_1_prior,
        DENSE_RANK() OVER w_second_played_at_2_to_date_l AS ms_played_share_of_played_at_to_date_dense_rank
    FROM first_rows
    WINDOW w_second_played_at_2_to_date_l AS (PARTITION BY TO_DATE(second_played_at_2), listener_id ORDER BY ms_played_share_of_played_at_to_date DESC)
),
totals AS (
    -- Group-level totals
    SELECT
        second_played_at_2_quartile,
        second_played_at_2_decile,
        ROUND(SUM(rows_in_listener_id_max_3_prior) / NULLIF(SUM(distinct_track_id_in_device_distinct_track_id_in), 0), 3) AS rows_in_listener_id_max_3_prior_to_distinct_trac,
        SUM(distinct_track_id_in_device_distinct_track_id_in) FILTER (WHERE device IN ('speaker', 'desktop', 'car')) AS distinct_track_id_in_device_distinct_track_id_in_2,
        COUNT(DISTINCT device) AS distinct_devices,
        SUM(ms_played_share_of_played_at_to_date_dense_rank) AS total_ms_played_share_of_played_at_to_date_dense,
        COUNT(*) AS n_rows
    FROM sequenced
    WHERE ms_played_share_of_played_at_to_date < 0.6
    GROUP BY second_played_at_2_quartile, second_played_at_2_decile
)
SELECT
    second_played_at_2_quartile,
    second_played_at_2_decile,
    rows_in_listener_id_max_3_prior_to_distinct_trac,
    distinct_track_id_in_device_distinct_track_id_in_2,
    total_ms_played_share_of_played_at_to_date_dense,
    n_rows
FROM totals
ORDER BY second_played_at_2_quartile, rows_in_listener_id_max_3_prior_to_distinct_trac DESC
