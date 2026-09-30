WITH matched AS (
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
joined_listener AS (
    -- Join listeners
    SELECT s.*, l.plan AS listener_plan, l.country AS listener_country
    FROM matched s
    LEFT JOIN listeners l ON l.id = s.listener_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT prior_rows_12h, listener_country, prior_ms_played_12h
    FROM joined_listener
) src
PIVOT (
    MIN(prior_ms_played_12h)
    FOR listener_country IN ('JP', 'KR', 'TH')
)
