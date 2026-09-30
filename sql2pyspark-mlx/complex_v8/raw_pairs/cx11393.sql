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
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label,
        HOUR(played_at) AS played_at_hour
    FROM covered
)
SELECT
    id,
    listener_id,
    prior_rows_1d,
    prior_ms_played_1d,
    played_at_hour,
    LEAST(prior_ms_played_1d, played_at_hour) AS least_prior_ms_played_1d_played_at_hour,
    DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at
FROM tagged
WHERE prior_rows_1d BETWEEN 2 AND 6
