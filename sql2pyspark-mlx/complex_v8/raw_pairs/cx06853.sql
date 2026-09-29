WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.ms_played) AS prior_ms_played_3d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
joined_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.prior_rows_3d,
        s.prior_ms_played_3d,
        l.country AS listener_country,
        l.username AS listener_username
    FROM in_window s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
listener_country_stats AS (
    -- Avg prior_rows_3d per listener_country
    SELECT listener_country, AVG(prior_rows_3d) AS avg_prior_rows_3d
    FROM joined_listener
    GROUP BY listener_country
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_prior_rows_3d,
        ROUND(s.prior_rows_3d - g.avg_prior_rows_3d, 2) AS prior_rows_3d_minus_avg
    FROM joined_listener s
    JOIN listener_country_stats g ON g.listener_country = s.listener_country
    WHERE s.prior_rows_3d > g.avg_prior_rows_3d
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT listener_id, listener_country, prior_rows_3d
    FROM above_average
) src
PIVOT (
    SUM(prior_rows_3d)
    FOR listener_country IN ('BR', 'UK', 'TH')
)
