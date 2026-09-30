WITH matched AS (
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
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_ym,
        MIN(played_at) AS first_played_at,
        SUM(prior_rows_7d) AS total_prior_rows_7d
    FROM matched
    WHERE HOUR(played_at) BETWEEN 8 AND 11
    GROUP BY DATE_FORMAT(played_at, 'yyyy-MM')
)
-- Final projection
SELECT
    played_at_ym,
    first_played_at,
    total_prior_rows_7d,
    YEAR(first_played_at) AS first_played_at_year,
    LOWER(TRIM(played_at_ym)) AS played_at_ym_clean
FROM rollup_base
ORDER BY first_played_at, total_prior_rows_7d NULLS LAST, played_at_ym
LIMIT 10
