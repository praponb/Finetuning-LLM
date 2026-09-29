WITH base AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        LENGTH(country) AS country_length
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-07-17 00:00:00'
      AND country IS NOT NULL
),
valid AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        country,
        COALESCE(country_length, 0) AS country_length_filled,
        COALESCE(browser, 'NONE') AS browser_filled
    FROM base
    WHERE started_at > TO_DATE('2023-07-07')
      AND country IS NOT NULL
),
base_sessions AS (
    SELECT
        id,
        customer_id,
        country,
        browser_filled,
        LENGTH(browser) AS browser_length,
        COALESCE(browser, 'N/A') AS browser_filled_2
    FROM valid
    WHERE browser = 'Firefox'
),
with_latest AS (
    SELECT
        s.id,
        s.browser_length,
        s.browser_filled_2,
        top1.page
    FROM base_sessions s
    JOIN LATERAL (
            SELECT c.page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
matched_keys AS (
    SELECT id
    FROM with_latest
    INTERSECT
    SELECT id
    FROM sessions
    WHERE customer_id IS NOT NULL
),
verified AS (
    SELECT s.*
    FROM with_latest s
    INNER JOIN matched_keys k ON s.id = k.id
),
with_windows AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY page ORDER BY browser_length NULLS FIRST) AS browser_length_dense_rank
    FROM verified
),
not_flagged AS (
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.page = '/home'
        )
)
SELECT
    browser_length,
    browser_filled_2,
    browser_length_dense_rank
FROM not_flagged
ORDER BY browser_length_dense_rank DESC;
