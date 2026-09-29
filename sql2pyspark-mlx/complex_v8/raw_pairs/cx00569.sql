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
without_matches AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM joined_ranges s
    WHERE EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.listener_id
              AND l.birth_year <= 1988
        )
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM without_matches s
    WHERE s.listener_id IN (
            SELECT p.listener_id
            FROM plays p
            WHERE p.device NOT IN ('desktop', 'car')
        )
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ROUND(PERCENT_RANK() OVER w_listener_id_prior_rows_7d_by, 3) AS played_at_percent_rank,
        LAG(prior_ms_played_7d, 1, 0) OVER (PARTITION BY listener_id, prior_rows_7d ORDER BY played_at DESC NULLS FIRST, id) AS prev_prior_ms_played_7d,
        ROUND(CUME_DIST() OVER w_listener_id_prior_rows_7d_by_2, 3) AS played_at_cume_dist,
        RANK() OVER (PARTITION BY listener_id, prior_rows_7d ORDER BY played_at DESC NULLS FIRST, id) AS played_at_rank,
        ROUND(AVG(prior_rows_7d) OVER (PARTITION BY listener_id, prior_rows_7d), 2) AS listener_id_prior_rows_7d_avg_prior_rows_7d
    FROM screened
    WINDOW w_listener_id_prior_rows_7d_by AS (PARTITION BY listener_id, prior_rows_7d ORDER BY played_at DESC NULLS FIRST, id), w_listener_id_prior_rows_7d_by_2 AS (PARTITION BY listener_id, prior_rows_7d ORDER BY played_at DESC NULLS FIRST)
)
SELECT
    id,
    played_at_percent_rank,
    prev_prior_ms_played_7d,
    played_at_cume_dist,
    played_at_rank
FROM windowed
WHERE played_at_rank > 3
ORDER BY prev_prior_ms_played_7d DESC NULLS FIRST, id
LIMIT 5
