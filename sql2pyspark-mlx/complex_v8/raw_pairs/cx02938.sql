WITH prepared_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        played_at + INTERVAL 1 DAYS AS played_at_plus_1d,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label
    FROM plays
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device_or_unknown,
        is_skipped_label,
        MAX(ms_played) OVER (PARTITION BY YEAR(played_at)) AS played_at_year_max_ms_played,
        ROUND(ms_played - AVG(ms_played) OVER w_played_at_year, 2) AS ms_played_vs_played_at_year_avg,
        ROUND(AVG(ms_played) OVER (PARTITION BY device, source ORDER BY ms_played ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_2,
        SUM(ms_played) OVER (PARTITION BY YEAR(played_at)) AS played_at_year_sum_ms_played,
        LAST_VALUE(played_at_plus_1d) IGNORE NULLS OVER (ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at_plus_1d
    FROM prepared_plays
    WINDOW w_played_at_year AS (PARTITION BY YEAR(played_at))
)
-- Subtotals with ROLLUP
SELECT
    track_id,
    listener_id,
    ROUND(AVG(played_at_year_max_ms_played), 2) AS avg_played_at_year_max_ms_played,
    ROUND(SUM(ms_played_avg_2) / NULLIF(SUM(ms_played_vs_played_at_year_avg), 0), 3) AS ms_played_avg_2_to_ms_played_vs_played_at_year_a
FROM sequenced
GROUP BY ROLLUP(track_id, listener_id)
ORDER BY track_id NULLS LAST, listener_id NULLS LAST
