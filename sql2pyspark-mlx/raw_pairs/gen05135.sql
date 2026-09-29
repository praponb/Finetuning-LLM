WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT(device, '/', country) AS device_country
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-07-22 00:00:00'
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.browser,
        s.country,
        x.event_time
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
base_sessions AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(country, 1, 3) AS country_prefix,
        CONCAT_WS('-', browser, country) AS browser_country_key,
        COALESCE(country, 'OTHER') AS country_filled
    FROM with_last
    WHERE country IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base_sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.session_id IN (15, 23)
        )
),
matched_keys AS (
    -- Set difference on keys
    SELECT country, browser
    FROM eligible
    EXCEPT
    SELECT country, browser
    FROM sessions
    WHERE customer_id IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    INNER JOIN matched_keys k ON s.country = k.country AND s.browser = k.browser
),
eligible_sessions AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time <= s.event_time
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible_sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_type = 'add_to_cart'
        )
)
SELECT
    id,
    browser,
    event_time,
    browser_country_key
FROM kept
ORDER BY browser DESC, id ASC;
