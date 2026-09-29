WITH prepared AS (
    -- Scope the plays rows
    SELECT *
    FROM plays
    WHERE device <> 'phone'
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        TO_DATE(played_at) AS played_at_date,
        LENGTH(TRIM(source)) AS source_len
    FROM prepared
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        played_at,
        ms_played,
        device,
        source,
        source_len,
        ROUND(CUME_DIST() OVER (PARTITION BY source, is_skipped ORDER BY ms_played, id), 3) AS ms_played_cume_dist,
        COUNT(*) OVER w_is_skipped_listener_id AS rows_in_is_skipped_listener_id,
        SIZE(COLLECT_SET(track_id) OVER w_is_skipped_listener_id) AS distinct_track_id_in_is_skipped_listener_id,
        ROUND(source_len / NULLIF(SUM(source_len) OVER (PARTITION BY is_skipped, listener_id), 0), 4) AS source_len_share_of_is_skipped_listener_id,
        MAX(source_len) OVER (PARTITION BY source ORDER BY source_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_source_len_so_far,
        DATEDIFF(played_at_date, LAG(played_at_date) OVER (PARTITION BY track_id, source_len ORDER BY ms_played NULLS LAST, id)) AS days_since_prev_played_at_date
    FROM derived
    WINDOW w_is_skipped_listener_id AS (PARTITION BY is_skipped, listener_id)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        source_len,
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        SUM(rows_in_is_skipped_listener_id) AS total_rows_in_is_skipped_listener_id,
        COUNT(*) AS row_count,
        MAX(ms_played) AS max_ms_played,
        MIN(played_at) AS first_played_at,
        COUNT(*) FILTER (WHERE source IS NOT NULL) AS n_source_is_not_null,
        ROUND(SUM(source_len_share_of_is_skipped_listener_id) / NULLIF(SUM(ms_played), 0), 3) AS source_len_share_of_is_skipped_listener_id_to_ms
    FROM with_windows
    GROUP BY source_len, DATE_TRUNC('MONTH', played_at)
)
-- Final projection
SELECT
    source_len,
    played_at_month,
    total_rows_in_is_skipped_listener_id,
    first_played_at,
    n_source_is_not_null,
    source_len_share_of_is_skipped_listener_id_to_ms,
    LEAST(max_ms_played, n_source_is_not_null) AS least_max_ms_played_n_source_is_not_null,
    DATE_TRUNC('HOUR', first_played_at) AS first_played_at_hour
FROM grouped
WHERE total_rows_in_is_skipped_listener_id BETWEEN 31 AND 96
ORDER BY first_played_at_hour, source_len_share_of_is_skipped_listener_id_to_ms
