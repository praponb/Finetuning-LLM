WITH recent_plays AS (
    -- Keep valid rows and derive helper columns
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
    WHERE source IN ('radio', 'album', 'playlist')
      AND track_id IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        played_at,
        ms_played,
        source,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (PARTITION BY listener_id, YEAR(played_at) ORDER BY played_at DESC, id))) / 60, 1) AS minutes_since_prev_played_at,
        SUM(ms_played) OVER (PARTITION BY listener_id, YEAR(played_at) ORDER BY played_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ms_played_sum_3,
        LEAD(ms_played) OVER (PARTITION BY listener_id, is_skipped ORDER BY played_at ASC, id) AS next_ms_played,
        LAST_VALUE(played_at) OVER (PARTITION BY listener_id, YEAR(played_at) ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY listener_id, is_skipped ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at
    FROM recent_plays
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        ms_played,
        source,
        next_ms_played,
        last_played_at,
        second_played_at,
        MIN(ms_played_sum_3) OVER (PARTITION BY source) AS source_min_ms_played_sum_3,
        next_ms_played - LAG(next_ms_played) OVER w_all_by_second_played_at AS next_ms_played_change
    FROM ranked
    WINDOW w_all_by_second_played_at AS (ORDER BY second_played_at, id)
)
SELECT
    id,
    ms_played,
    source,
    next_ms_played,
    last_played_at,
    second_played_at,
    source_min_ms_played_sum_3,
    next_ms_played_change
FROM metrics
WHERE last_played_at < TIMESTAMP '2025-03-02 10:00:00'
ORDER BY last_played_at DESC, id
LIMIT 5
