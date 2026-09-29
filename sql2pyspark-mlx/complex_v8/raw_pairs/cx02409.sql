WITH scoped_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        DAYOFWEEK(played_at) AS played_at_dayofweek,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag
    FROM plays
    WHERE ms_played >= 73625
),
enriched AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        s.is_skipped_flag,
        l.signup_date AS listener_signup_date,
        l.country AS listener_country
    FROM scoped_plays s
    JOIN listeners l ON l.id = s.listener_id
    WHERE l.country = 'KR'
),
stats AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', listener_signup_date) AS listener_signup_date_month,
        is_skipped_flag,
        MAX(listener_signup_date) AS last_listener_signup_date,
        COUNT(DISTINCT listener_country) AS distinct_listener_countrys,
        MAX(is_skipped_flag) AS max_is_skipped_flag,
        PERCENTILE_APPROX(is_skipped_flag, 0.5) AS median_is_skipped_flag,
        ROUND(100 * COUNT(CASE WHEN listener_country = 'KR' THEN 1 END) / COUNT(*), 1) AS pct_listener_country_kr,
        COUNT(*) AS n_rows
    FROM enriched
    GROUP BY DATE_TRUNC('MONTH', listener_signup_date), is_skipped_flag
    HAVING COUNT(*) >= 2
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY median_is_skipped_flag), 3) AS median_is_skipped_flag_percent_rank,
        LEAD(is_skipped_flag, 1, 0) OVER (ORDER BY median_is_skipped_flag, listener_signup_date_month, is_skipped_flag) AS next_is_skipped_flag
    FROM stats
),
windowed AS (
    -- Previous / next values per key
    SELECT
        listener_signup_date_month,
        is_skipped_flag,
        distinct_listener_countrys,
        max_is_skipped_flag,
        median_is_skipped_flag,
        pct_listener_country_kr,
        n_rows,
        median_is_skipped_flag_percent_rank,
        NTILE(4) OVER (PARTITION BY MONTH(last_listener_signup_date), max_is_skipped_flag ORDER BY n_rows DESC, listener_signup_date_month, is_skipped_flag) AS n_rows_quartile,
        NTILE(3) OVER (PARTITION BY MONTH(last_listener_signup_date), max_is_skipped_flag ORDER BY n_rows DESC, listener_signup_date_month, is_skipped_flag) AS n_rows_tercile,
        LEAD(is_skipped_flag) OVER w_last_listener_signup_date_mo AS next_is_skipped_flag_2
    FROM metrics
    WINDOW w_last_listener_signup_date_mo AS (PARTITION BY MONTH(last_listener_signup_date), max_is_skipped_flag ORDER BY n_rows DESC, listener_signup_date_month, is_skipped_flag)
)
-- Final projection
SELECT
    listener_signup_date_month,
    distinct_listener_countrys,
    pct_listener_country_kr,
    n_rows,
    median_is_skipped_flag_percent_rank,
    n_rows_quartile,
    n_rows_tercile,
    next_is_skipped_flag_2
FROM windowed
ORDER BY listener_signup_date_month, distinct_listener_countrys
