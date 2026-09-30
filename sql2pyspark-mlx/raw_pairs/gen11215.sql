WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        started_at,
        country,
        CASE WHEN device = 'tablet' THEN 1 ELSE 0 END is_tablet,
        LENGTH(country) country_length
    FROM sessions
    WHERE started_at > TIMESTAMP '2023-08-30 10:00:00'
      AND country = 'DE'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY country ORDER BY country_length DESC NULLS FIRST) country_length_rank,
        SUM(country_length) OVER w_grp running_country_length,
        RANK() OVER (PARTITION BY customer_id ORDER BY is_tablet DESC NULLS LAST) is_tablet_rank
    FROM base
    WINDOW w_grp AS (PARTITION BY country, customer_id ORDER BY is_tablet, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.started_at,
        s.country_length,
        s.country_length_rank,
        s.running_country_length,
        s.is_tablet_rank,
        latest.duration_sec,
        latest.web_event_page
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.duration_sec, c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows_sessions AS (
    -- Ranking and gap features
    SELECT
        id,
        started_at,
        country_length,
        running_country_length,
        is_tablet_rank,
        duration_sec,
        ROUND(country_length_rank / SUM(country_length_rank) OVER (PARTITION BY web_event_page), 4) country_length_rank_share,
        SUM(duration_sec) OVER (PARTITION BY web_event_page ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_duration_sec,
        MAX(running_country_length) OVER (ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) running_country_length_max_30d
    FROM with_last
    WHERE web_event_page IN ('/search', '/product')
)
-- Running and moving aggregates
SELECT
    id,
    started_at,
    country_length,
    is_tablet_rank,
    running_country_length_max_30d,
    SUM(running_country_length_max_30d) OVER (ORDER BY country_length_rank_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_running_country_length_max_30d,
    COUNT(running_country_length_max_30d) OVER (ORDER BY country_length_rank_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_running_country_length_max_30d,
    MAX(running_country_length_max_30d) OVER (ORDER BY country_length_rank_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_running_country_length_max_30d,
    AVG(running_country_length_max_30d) OVER (ORDER BY country_length_rank_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_running_country_length_max_30d
FROM with_windows_sessions
