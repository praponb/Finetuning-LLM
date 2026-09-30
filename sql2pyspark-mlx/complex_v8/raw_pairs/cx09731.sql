WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.ms_played) AS prior_ms_played_12h
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.listener_id, a.played_at
),
enriched_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.prior_rows_12h,
        l.birth_year AS listener_birth_year
    FROM covered s
    JOIN listeners l ON l.id = s.listener_id
    WHERE l.birth_year > 1966
),
without_matches AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM enriched_listener s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.is_skipped = TRUE
              AND p.source IN ('search', 'radio', 'album')
        )
),
rollup_base AS (
    -- Group-level totals
    SELECT
        prior_rows_12h,
        TO_DATE(played_at) AS played_at_day,
        listener_id,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids,
        MIN(played_at) AS first_played_at
    FROM without_matches
    WHERE prior_rows_12h < 6
    GROUP BY 1, 2, 3
)
-- Final projection
SELECT
    prior_rows_12h,
    played_at_day,
    listener_id,
    distinct_listener_ids,
    first_played_at,
    LEAST(distinct_listener_ids, prior_rows_12h) AS least_distinct_listener_ids_prior_rows_12h
FROM rollup_base
WHERE listener_id >= 16
ORDER BY played_at_day, distinct_listener_ids
