WITH filtered_plays AS (
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
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at
    FROM plays
    WHERE (listener_id IS NOT NULL OR device = 'tv')
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (ORDER BY ms_played, id) AS ms_played_rank,
        SUM(ms_played) OVER (ORDER BY ms_played, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS ms_played_sum_1_prior,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY listener_id, device ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        LAG(played_at, 2) OVER (PARTITION BY source, listener_id ORDER BY days_since_played_at DESC, id) AS prev2_played_at,
        MIN(ms_played) OVER (PARTITION BY device, is_skipped) AS device_is_skipped_min_ms_played,
        ROW_NUMBER() OVER w_all_by_ms_played AS ms_played_seq,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_id, device ORDER BY ms_played ASC), 3) AS ms_played_cume_dist,
        LAST_VALUE(days_since_played_at) IGNORE NULLS OVER (ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_played_at
    FROM filtered_plays
    WINDOW w_all_by_ms_played AS (ORDER BY ms_played, id)
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    ms_played,
    device,
    days_since_played_at,
    second_played_at,
    prev2_played_at,
    ms_played_seq,
    ms_played_cume_dist,
    last_known_days_since_played_at,
    ABS(days_since_played_at - 455) AS days_since_played_at_distance
FROM with_trends
ORDER BY ms_played NULLS LAST
