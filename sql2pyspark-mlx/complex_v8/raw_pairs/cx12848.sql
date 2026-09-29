WITH base AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source
    FROM plays
    WHERE track_id >= 28
      AND device NOT IN ('car', 'speaker')
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY played_at DESC, id) AS rn,
        NTILE(5) OVER w_all_by_played_at_2 AS played_at_quintile,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (ORDER BY played_at DESC, id))) / 60, 1) AS minutes_since_prev_played_at,
        RANK() OVER (ORDER BY played_at DESC, id) AS played_at_rank,
        ROUND(CUME_DIST() OVER (ORDER BY played_at DESC), 3) AS played_at_cume_dist,
        NTH_VALUE(played_at, 2) OVER (ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        NTILE(5) OVER (PARTITION BY track_id, is_skipped ORDER BY played_at NULLS LAST, id) AS played_at_quintile_2,
        NTILE(10) OVER (PARTITION BY track_id, is_skipped ORDER BY played_at NULLS LAST, id) AS played_at_decile,
        ROW_NUMBER() OVER (ORDER BY played_at ASC, id) AS rn_2,
        ROUND(PERCENT_RANK() OVER w_track_id_is_skipped_by_playe, 3) AS played_at_percent_rank,
        RANK() OVER (PARTITION BY track_id, is_skipped ORDER BY played_at NULLS LAST, id) AS played_at_rank_2,
        ROUND(CUME_DIST() OVER w_all_by_played_at, 3) AS played_at_cume_dist_2,
        NTILE(3) OVER (ORDER BY played_at ASC, id) AS played_at_tercile
    FROM base
    WINDOW w_all_by_played_at_2 AS (ORDER BY played_at ASC, id), w_track_id_is_skipped_by_playe AS (PARTITION BY track_id, is_skipped ORDER BY played_at NULLS LAST, id), w_all_by_played_at AS (ORDER BY played_at DESC, id)
)
-- Final projection
SELECT
    id,
    listener_id,
    played_at,
    device,
    is_skipped,
    source,
    rn,
    played_at_quintile,
    played_at_rank,
    played_at_cume_dist,
    played_at_quintile_2,
    played_at_decile,
    played_at_percent_rank,
    played_at_rank_2,
    played_at_cume_dist_2,
    played_at_tercile,
    CASE WHEN played_at_cume_dist >= 0.8 THEN 'HIGH' WHEN played_at_cume_dist >= 0.3 THEN 'MEDIUM' ELSE 'LOW' END AS played_at_cume_dist_tier,
    CONCAT_WS(' / ', device, source) AS device_source_label
FROM ranked
WHERE second_played_at < TIMESTAMP '2025-02-18 12:00:00'
ORDER BY played_at_quintile_2 DESC, id
LIMIT 5
