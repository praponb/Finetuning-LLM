WITH valid AS (
    -- Scope the plays rows
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
    WHERE listener_id >= 9
),
enriched AS (
    -- Attach track attributes
    SELECT
        s.*,
        t.release_date AS track_release_date,
        l.signup_date AS listener_signup_date,
        l.country AS listener_country
    FROM valid s
    LEFT JOIN tracks t ON t.id = s.track_id
    LEFT JOIN listeners l ON l.id = s.listener_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        track_release_date,
        listener_signup_date,
        COUNT(*) OVER (PARTITION BY listener_id, source) AS rows_in_listener_id_source,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY DATE_TRUNC('MONTH', listener_signup_date), listener_id), 2) AS ms_played_vs_listener_signup_date_month_listener,
        LAST_VALUE(listener_signup_date) IGNORE NULLS OVER (ORDER BY track_release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_listener_signup_date,
        ROUND(AVG(ms_played) OVER (PARTITION BY device ORDER BY track_release_date DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_2,
        DATEDIFF(track_release_date, LAG(track_release_date) OVER w_device_by_track_release_date_2) AS days_since_prev_track_release_date,
        NTILE(5) OVER w_all_by_track_release_date AS track_release_date_quintile,
        NTILE(3) OVER (PARTITION BY device ORDER BY track_release_date DESC, id) AS track_release_date_tercile,
        SUM(ms_played) OVER (ORDER BY track_release_date DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ms_played_sum_2,
        FIRST_VALUE(track_release_date) IGNORE NULLS OVER (ORDER BY track_release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_track_release_date,
        ms_played - LAG(ms_played) OVER (PARTITION BY device ORDER BY track_release_date DESC, id) AS ms_played_change,
        ROUND(CUME_DIST() OVER w_all_by_track_release_date_ti, 3) AS track_release_date_cume_dist,
        SUM(ms_played) OVER (PARTITION BY is_skipped, MONTH(played_at)) AS is_skipped_played_at_month_sum_ms_played
    FROM enriched
    WINDOW w_device_by_track_release_date_2 AS (PARTITION BY device ORDER BY track_release_date DESC, id), w_all_by_track_release_date AS (ORDER BY track_release_date DESC, id), w_all_by_track_release_date_ti AS (ORDER BY track_release_date DESC)
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        listener_id,
        played_at,
        ms_played,
        device,
        source,
        track_release_date,
        listener_signup_date,
        rows_in_listener_id_source,
        ms_played_vs_listener_signup_date_month_listener,
        last_known_listener_signup_date,
        days_since_prev_track_release_date,
        track_release_date_quintile,
        track_release_date_tercile,
        ms_played_sum_2,
        first_known_track_release_date,
        ms_played_change,
        is_skipped_played_at_month_sum_ms_played,
        ms_played_avg_2 + ms_played_change AS ms_played_avg_2_plus_ms_played_change,
        UPPER(TRIM(source)) AS source_clean,
        ROUND(ms_played_avg_2 * 1.07, 2) AS ms_played_avg_2_adj
    FROM ranked
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(ms_played_change) OVER w_track_release_date_month, 2) AS track_release_date_month_avg_ms_played_change,
        LAG(track_release_date_tercile, 3, 0) OVER (PARTITION BY listener_id ORDER BY listener_signup_date DESC, id) AS prev3_track_release_date_tercile,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY DATE_TRUNC('MONTH', track_release_date)), 0), 4) AS ms_played_share_of_track_release_date_month,
        SUM(ms_played_avg_2_adj) OVER (PARTITION BY source ORDER BY last_known_listener_signup_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played_avg_2_adj,
        FIRST_VALUE(rows_in_listener_id_source) IGNORE NULLS OVER (PARTITION BY listener_id ORDER BY listener_signup_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rows_in_listener_id_source
    FROM scored
    WINDOW w_track_release_date_month AS (PARTITION BY DATE_TRUNC('MONTH', track_release_date))
)
-- Final projection
SELECT
    id,
    listener_id,
    played_at,
    ms_played,
    device,
    source,
    track_release_date,
    listener_signup_date,
    rows_in_listener_id_source,
    ms_played_vs_listener_signup_date_month_listener,
    last_known_listener_signup_date,
    track_release_date_quintile,
    track_release_date_tercile,
    ms_played_avg_2_plus_ms_played_change,
    source_clean,
    ms_played_avg_2_adj,
    prev3_track_release_date_tercile,
    ms_played_share_of_track_release_date_month,
    running_ms_played_avg_2_adj,
    first_known_rows_in_listener_id_source
FROM with_windows
ORDER BY source_clean, source
