WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source
    FROM plays
    WHERE (device IN ('car', 'speaker') OR is_skipped = FALSE)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        track_id,
        played_at,
        source,
        NTH_VALUE(played_at, 2) OVER (ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        LEAD(played_at) OVER (PARTITION BY listener_id ORDER BY played_at DESC, id) AS next_played_at,
        DENSE_RANK() OVER w_listener_id_by_played_at_tie AS played_at_dense_rank,
        LAG(played_at, 2) OVER w_all_by_played_at AS prev2_played_at,
        NTILE(10) OVER (PARTITION BY listener_id ORDER BY played_at DESC, id) AS played_at_decile,
        LAG(played_at) OVER w_device_listener_id_by_played AS prev_played_at,
        RANK() OVER (PARTITION BY device, listener_id ORDER BY played_at) AS played_at_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY device, listener_id ORDER BY played_at, id), 3) AS played_at_cume_dist
    FROM scoped_plays
    WINDOW w_listener_id_by_played_at_tie AS (PARTITION BY listener_id ORDER BY played_at DESC), w_all_by_played_at AS (ORDER BY played_at DESC, id), w_device_listener_id_by_played AS (PARTITION BY device, listener_id ORDER BY played_at, id)
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        track_id,
        source,
        second_played_at,
        next_played_at,
        played_at_dense_rank,
        played_at_decile,
        prev_played_at,
        played_at_rank,
        played_at_cume_dist,
        ROUND((UNIX_TIMESTAMP(second_played_at) - UNIX_TIMESTAMP(prev2_played_at)) / 3600, 1) AS hours_prev2_played_at_to_second_played_at,
        CONCAT(source, '#', CAST(played_at_decile AS STRING)) AS source_ref,
        FLOOR(played_at_rank / 2) * 2 AS played_at_rank_bucket
    FROM windowed
)
-- Final projection
SELECT
    id,
    track_id,
    source,
    next_played_at,
    played_at_dense_rank,
    played_at_decile,
    prev_played_at,
    played_at_cume_dist,
    hours_prev2_played_at_to_second_played_at,
    source_ref,
    played_at_rank_bucket,
    CONCAT(source, '#', CAST(track_id AS STRING)) AS source_ref_2
FROM labeled
ORDER BY next_played_at
