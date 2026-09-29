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
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM joined_ranges s
    WHERE s.listener_id IN (
            SELECT p.listener_id
            FROM plays p
            WHERE p.ms_played > 73625
        )
)
SELECT
    prior_rows_7d,
    listener_id,
    PERCENTILE_APPROX(prior_rows_7d, 0.5) AS median_prior_rows_7d
FROM qualified
GROUP BY CUBE(prior_rows_7d, listener_id)
ORDER BY prior_rows_7d NULLS LAST, listener_id NULLS LAST
