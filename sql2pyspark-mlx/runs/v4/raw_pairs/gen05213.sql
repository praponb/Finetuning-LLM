WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        page,
        duration_sec,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        COALESCE(duration_sec, 0) AS duration_sec_filled_2,
        COALESCE(page, 'DIRECT') AS resolved_page
    FROM web_events
    WHERE event_time >= DATE '2023-07-12'
),
joined AS (
    -- Attach sessions attributes
    SELECT
        a.*,
        p.country,
        p.browser
    FROM cleaned a
    INNER JOIN sessions p
        ON a.session_id = p.id
),
cleaned_web_events AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(country, 'unassigned') AS country_filled,
        CONCAT(browser, ' ', country) AS browser_country,
        COALESCE(browser, 'N/A') AS resolved_browser
    FROM joined
    WHERE page IN ('/product', '/checkout')
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        page,
        browser,
        country,
        duration_sec_filled_2
    FROM cleaned_web_events
) src
PIVOT (
    SUM(duration_sec_filled_2) AS value_sum,
    COUNT(1) AS n
    FOR country IN ('DE', 'UK', 'JP')
);
