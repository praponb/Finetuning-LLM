WITH base_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label,
        CONCAT(source, '#', CAST(ms_played AS STRING)) AS source_ref
    FROM plays
    WHERE (source = 'playlist' OR ms_played BETWEEN 108979 AND 299017)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (ORDER BY played_at, id))) / 60, 1) AS minutes_since_prev_played_at,
        LAST_VALUE(played_at) IGNORE NULLS OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at,
        ROUND(CUME_DIST() OVER (ORDER BY played_at, id), 3) AS played_at_cume_dist,
        DENSE_RANK() OVER w_all_by_played_at AS played_at_dense_rank
    FROM base_plays
    WINDOW w_all_by_played_at AS (ORDER BY played_at, id)
),
first_rows AS (
    -- Top 2 per group
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        played_at_label,
        source_ref,
        first_known_played_at,
        minutes_since_prev_played_at,
        last_known_played_at,
        played_at_cume_dist
    FROM metrics
    WHERE played_at_dense_rank <= 2
)
SELECT id, metric, reading
FROM (
    SELECT id, played_at_cume_dist, minutes_since_prev_played_at
    FROM first_rows
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric IN (played_at_cume_dist, minutes_since_prev_played_at)
) unp
ORDER BY id, metric
