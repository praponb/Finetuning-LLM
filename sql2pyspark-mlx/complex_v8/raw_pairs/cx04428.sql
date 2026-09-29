WITH in_window AS (
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
per_group AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_ym,
        listener_id,
        SUM(prior_ms_played_12h) AS total_prior_ms_played_12h,
        COUNT(*) AS row_count,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(AVG(prior_rows_12h), 2) AS avg_prior_rows_12h
    FROM in_window
    GROUP BY DATE_FORMAT(played_at, 'yyyy-MM'), listener_id
)
SELECT
    played_at_ym,
    total_prior_ms_played_12h,
    row_count,
    distinct_listener_ids,
    distinct_ids,
    avg_prior_rows_12h
FROM per_group
