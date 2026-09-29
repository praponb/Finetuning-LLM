WITH with_dims AS (
    SELECT
        a.*,
        b.browser,
        b.device,
        b.country
    FROM web_events a
    LEFT JOIN sessions b
        ON a.session_id = b.id
    WHERE b.device = 'mobile'
),
valid AS (
    SELECT
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        browser,
        device,
        country,
        LOWER(browser) AS browser_lower,
        COALESCE(page, 'N/A') AS page_filled
    FROM with_dims
    WHERE page IS NOT DISTINCT FROM '/product'
),
filtered AS (
    SELECT
        *,
        LOWER(page) AS page_lower,
        CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END AS is_page_view
    FROM valid
    WHERE event_time > TO_TIMESTAMP('2023-07-17 15:00:00')
),
scored AS (
    SELECT
        *,
        MAX(is_page_view) OVER (PARTITION BY YEAR(event_time), browser) AS event_time_year_max_is_page_view,
        LAG(is_page_view, 1) OVER (PARTITION BY country, page ORDER BY event_time, id) AS prev_is_page_view,
        RANK() OVER (PARTITION BY page, event_type ORDER BY duration_sec NULLS LAST) AS duration_sec_rank
    FROM filtered
),
metrics AS (
    SELECT
        *,
        ROW_NUMBER() OVER w_hist AS row_seq,
        SUM(duration_sec_rank) OVER (PARTITION BY browser) AS browser_sum_duration_sec_rank
    FROM scored
    WINDOW w_hist AS (PARTITION BY country ORDER BY duration_sec_rank, id)
)
SELECT
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    COUNT(DISTINCT country) AS distinct_countries,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS duration_min
FROM metrics
GROUP BY 1
HAVING COUNT(DISTINCT country) > 1
ORDER BY duration_min DESC NULLS FIRST
