WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.ms_played) AS prior_ms_played_1d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        played_at,
        prior_rows_1d,
        prior_ms_played_1d,
        COUNT(*) OVER (PARTITION BY TO_DATE(played_at)) AS rows_in_played_at_to_date,
        ROUND(prior_rows_1d / NULLIF(SUM(prior_rows_1d) OVER (PARTITION BY TO_DATE(played_at)), 0), 4) AS prior_rows_1d_share_of_played_at_to_date,
        ROUND(AVG(prior_ms_played_1d) OVER (PARTITION BY TO_DATE(played_at)), 2) AS played_at_to_date_avg_prior_ms_played_1d
    FROM covered
)
-- Final projection
SELECT
    id,
    prior_rows_1d,
    rows_in_played_at_to_date,
    prior_rows_1d_share_of_played_at_to_date,
    played_at_to_date_avg_prior_ms_played_1d,
    ABS(prior_ms_played_1d - 1100000) AS prior_ms_played_1d_distance
FROM sequenced
ORDER BY prior_ms_played_1d_distance, id
LIMIT 5
