WITH valid_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        COALESCE(ms_played, 0) AS ms_played_filled,
        COALESCE(ms_played, 0) AS ms_played_filled_2
    FROM plays
    WHERE NOT is_skipped
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY device ORDER BY played_at ASC, id) AS played_at_dense_rank,
        LAST_VALUE(played_at) IGNORE NULLS OVER (ORDER BY ms_played_filled_2 NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at
    FROM valid_plays
),
with_trends_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_track_id_by_last_known_playe, 3) AS last_known_played_at_percent_rank,
        ROUND(AVG(played_at_dense_rank) OVER (PARTITION BY track_id ORDER BY last_known_played_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS played_at_dense_rank_avg_3
    FROM with_trends
    WINDOW w_track_id_by_last_known_playe AS (PARTITION BY track_id ORDER BY last_known_played_at, id)
)
SELECT *
FROM (
    SELECT listener_id, source, played_at_dense_rank
    FROM with_trends_2
) src
PIVOT (
    MIN(played_at_dense_rank)
    FOR source IN ('radio', 'playlist', 'search')
)
ORDER BY listener_id
