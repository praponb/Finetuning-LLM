WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        COALESCE(ms_played, 0) AS ms_played_filled,
        DATEDIFF(DATE '2025-12-31', played_at) AS days_since_played_at
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-01-31 12:00:00'
      AND track_id IS NOT NULL
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(days_since_played_at) OVER (PARTITION BY source, listener_id ORDER BY ms_played DESC, id) AS prev_days_since_played_at,
        SUM(ms_played) OVER (ORDER BY ms_played_filled DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ms_played_sum_7,
        ROUND(ms_played_filled - AVG(ms_played_filled) OVER (PARTITION BY source, listener_id), 2) AS ms_played_filled_vs_source_listener_id_avg,
        NTH_VALUE(days_since_played_at, 2) OVER (PARTITION BY source, listener_id ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_played_at
    FROM scoped
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        track_id,
        ms_played,
        source,
        ms_played_filled,
        days_since_played_at,
        prev_days_since_played_at,
        second_days_since_played_at,
        COUNT(*) OVER (PARTITION BY track_id) AS rows_in_track_id,
        DENSE_RANK() OVER (PARTITION BY source, track_id ORDER BY ms_played_filled_vs_source_listener_id_avg ASC) AS ms_played_filled_vs_source_listener_id_avg_dense
    FROM sequenced
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, amount
FROM (
    SELECT id, prev_days_since_played_at, days_since_played_at
    FROM ranked
) src
UNPIVOT (
    amount FOR metric_name IN (prev_days_since_played_at, days_since_played_at)
) unp
ORDER BY id, metric_name
