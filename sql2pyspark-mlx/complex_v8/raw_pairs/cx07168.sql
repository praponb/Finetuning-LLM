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
with_parent AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.prior_rows_3d,
        l.username AS listener_username,
        l.country AS listener_country
    FROM in_window s
    JOIN listeners l ON l.id = s.listener_id AND UPPER(TRIM(l.username)) LIKE 'C%'
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        listener_country,
        COUNT(*) AS cnt,
        ROUND(STDDEV(prior_rows_3d), 2) AS stddev_prior_rows_3d,
        MAX(played_at) AS last_played_at,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids,
        ARRAY_SORT(COLLECT_SET(listener_username)) AS listener_username_list
    FROM with_parent
    GROUP BY 1
    HAVING COUNT(*) >= 3
)
SELECT
    listener_country,
    cnt,
    stddev_prior_rows_3d,
    distinct_listener_ids,
    listener_username_list,
    LOWER(REGEXP_REPLACE(TRIM(listener_country), ' ', '_')) AS listener_country_slug,
    TO_DATE(last_played_at) AS last_played_at_date
FROM grouped
ORDER BY listener_country
