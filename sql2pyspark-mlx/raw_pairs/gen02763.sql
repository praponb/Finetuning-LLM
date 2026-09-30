WITH cleaned AS (
    SELECT
        *,
        INITCAP(TRIM(country)) AS country_title,
        COALESCE(country, 'N/A') AS country_filled
    FROM sessions
    WHERE device IN ('desktop', 'tablet')
      AND started_at > DATE '2023-06-12'
),
not_flagged AS (
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time >= s.started_at
        )
),
joined AS (
    SELECT
        e.id,
        e.customer_id,
        e.device,
        e.browser,
        e.country,
        e.country_title,
        e.country_filled,
        c.name
    FROM not_flagged e
    JOIN customers c
        ON e.customer_id = c.id
    WHERE c.country != 'UK'
),
with_latest AS (
    SELECT
        s.*,
        x.web_event_page,
        x.web_event_duration_sec
    FROM joined s
    JOIN LATERAL (
            SELECT c.page AS web_event_page, c.duration_sec AS web_event_duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    SELECT
        id,
        browser,
        country_filled,
        name,
        LAG(web_event_duration_sec, 2) OVER (ORDER BY web_event_duration_sec, id) AS prev_web_event_duration_sec,
        AVG(web_event_duration_sec) OVER (PARTITION BY device ORDER BY web_event_duration_sec, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS web_event_duration_sec_avg_6_prior,
        LAG(web_event_duration_sec, 2) OVER (PARTITION BY country ORDER BY web_event_duration_sec, id) AS prev_web_event_duration_sec_2
    FROM with_latest
),
base AS (
    SELECT
        id,
        browser,
        country_filled,
        name,
        prev_web_event_duration_sec,
        prev_web_event_duration_sec_2,
        SUBSTRING(name, 1, 2) AS name_prefix,
        INITCAP(TRIM(name)) AS name_title,
        COALESCE(browser, 'N/A') AS browser_filled
    FROM metrics
    WHERE browser IS NOT NULL
      AND web_event_duration_sec_avg_6_prior != 352
),
eligible AS (
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.session_id IN (3, 10, 21, 23)
        )
)
SELECT
    browser,
    CASE WHEN prev_web_event_duration_sec_2 >= 375 THEN 'LARGE' ELSE 'SMALL' END AS prev_web_event_duration_sec_2_tier,
    ROUND(SUM(prev_web_event_duration_sec) / NULLIF(COUNT(CASE WHEN browser = 'Safari' THEN 1 END), 0), 2) AS prev_web_event_duration_sec_per_safari,
    SUM(prev_web_event_duration_sec_2) AS total_prev_web_event_duration_sec_2,
    COUNT(DISTINCT id) AS distinct_ids,
    GROUPING(browser) AS gid
FROM eligible
GROUP BY GROUPING SETS ((browser, CASE WHEN prev_web_event_duration_sec_2 >= 375 THEN 'LARGE' ELSE 'SMALL' END), (browser), ())
ORDER BY prev_web_event_duration_sec_per_safari NULLS LAST;
