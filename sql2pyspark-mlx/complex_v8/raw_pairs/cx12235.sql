WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM plays
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        played_at,
        source,
        device_slug,
        device_or_unknown,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (PARTITION BY track_id, source ORDER BY played_at NULLS LAST, id))) / 60, 1) AS minutes_since_prev_played_at,
        LEAD(played_at, 3) OVER (ORDER BY played_at DESC NULLS FIRST, id) AS next3_played_at,
        ROUND(PERCENT_RANK() OVER w_all_by_played_at_ties, 3) AS played_at_percent_rank,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (ORDER BY played_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at,
        ROUND(PERCENT_RANK() OVER w_track_id_source_by_played_at_2, 3) AS played_at_percent_rank_2,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (ORDER BY played_at ASC, id))) / 60, 1) AS minutes_since_prev_played_at_2,
        LEAD(played_at) OVER w_track_id_source_by_played_at AS next_played_at,
        FIRST_VALUE(played_at) OVER (PARTITION BY source, listener_id ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_id ORDER BY played_at ASC), 3) AS played_at_cume_dist
    FROM cleaned
    WINDOW w_all_by_played_at_ties AS (ORDER BY played_at DESC NULLS FIRST), w_track_id_source_by_played_at_2 AS (PARTITION BY track_id, source ORDER BY played_at NULLS LAST), w_track_id_source_by_played_at AS (PARTITION BY track_id, source ORDER BY played_at NULLS LAST, id)
)
SELECT
    id,
    played_at,
    source,
    device_slug,
    device_or_unknown,
    next3_played_at,
    played_at_percent_rank,
    first_known_played_at,
    played_at_percent_rank_2,
    minutes_since_prev_played_at_2,
    next_played_at,
    first_played_at,
    played_at_cume_dist,
    LEAST(played_at_percent_rank_2, played_at_percent_rank) AS least_played_at_percent_rank_2_played_at_percent,
    INITCAP(TRIM(device_or_unknown)) AS device_or_unknown_clean
FROM sequenced
ORDER BY next_played_at DESC
