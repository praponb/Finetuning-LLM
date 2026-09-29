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
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (ORDER BY prior_rows_7d ASC NULLS LAST, id) AS prior_rows_7d_tercile,
        prior_rows_7d - LAG(prior_rows_7d) OVER w_all_by_prior_rows_7d_2 AS prior_rows_7d_change,
        MAX(prior_rows_7d) OVER w_prior_rows_7d AS prior_rows_7d_max_prior_rows_7d
    FROM matched
    WINDOW w_all_by_prior_rows_7d_2 AS (ORDER BY prior_rows_7d, id), w_prior_rows_7d AS (PARTITION BY prior_rows_7d)
),
with_details_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.prior_rows_7d,
        s.prior_rows_7d_tercile,
        s.prior_rows_7d_change,
        s.prior_rows_7d_max_prior_rows_7d,
        l.country AS listener_country,
        l.signup_date AS listener_signup_date,
        l.username AS listener_username
    FROM with_windows s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
summary AS (
    -- Group-level totals
    SELECT
        YEAR(played_at) AS played_at_year,
        ROUND(AVG(prior_rows_7d_tercile), 2) AS avg_prior_rows_7d_tercile,
        PERCENTILE_APPROX(prior_rows_7d_change, 0.5) AS median_prior_rows_7d_change,
        SUM(CASE WHEN listener_country NOT IN ('KR', 'TH') THEN 1 ELSE 0 END) AS n_listener_country_not_in_kr_t,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids,
        MAX(played_at) AS last_played_at,
        ARRAY_SORT(COLLECT_SET(listener_country)) AS listener_country_list
    FROM with_details_listener
    GROUP BY YEAR(played_at)
    HAVING AVG(prior_rows_7d_change) > -2
)
-- Native SQL UNPIVOT (wide to long)
SELECT played_at_year, measure, value
FROM (
    SELECT played_at_year, n_listener_country_not_in_kr_t, distinct_listener_ids
    FROM summary
) src
UNPIVOT (
    value FOR measure IN (n_listener_country_not_in_kr_t, distinct_listener_ids)
) unp
ORDER BY played_at_year, measure
