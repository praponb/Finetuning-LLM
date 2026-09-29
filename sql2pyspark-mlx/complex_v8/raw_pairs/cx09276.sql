WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.ms_played) AS prior_ms_played_7d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        played_at,
        prior_rows_7d,
        LEAD(prior_ms_played_7d) OVER (ORDER BY prior_ms_played_7d DESC, id) AS next_prior_ms_played_7d,
        NTILE(3) OVER (ORDER BY prior_ms_played_7d DESC, id) AS prior_ms_played_7d_tercile,
        ROUND(CUME_DIST() OVER (ORDER BY prior_ms_played_7d DESC), 3) AS prior_ms_played_7d_cume_dist,
        LEAD(played_at, 2) OVER (ORDER BY prior_ms_played_7d DESC, id) AS next2_played_at
    FROM joined_ranges
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        prior_rows_7d,
        next_prior_ms_played_7d,
        prior_ms_played_7d_tercile,
        prior_ms_played_7d_cume_dist,
        next2_played_at,
        COALESCE(prior_rows_7d, 0) AS prior_rows_7d_filled
    FROM sequenced
)
-- Subtotals with ROLLUP
SELECT
    prior_rows_7d_filled,
    prior_ms_played_7d_tercile,
    prior_rows_7d,
    MIN(prior_rows_7d) AS min_prior_rows_7d,
    COUNT(*) AS cnt,
    SUM(prior_rows_7d_filled) AS total_prior_rows_7d_filled
FROM labeled
GROUP BY ROLLUP(prior_rows_7d_filled, prior_ms_played_7d_tercile, prior_rows_7d)
ORDER BY prior_rows_7d_filled NULLS LAST, prior_ms_played_7d_tercile NULLS LAST, prior_rows_7d NULLS LAST
