WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.started_at < s.signup_date
        )
),
with_latest AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.city,
        s.segment,
        s.is_active,
        x.session_browser,
        x.session_country
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.browser AS session_browser, c.country AS session_country
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM accounts x
            WHERE x.customer_id = s.id
              AND x.customer_id IS NULL
        )
),
with_last AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.session_browser,
        s.session_country,
        top1.session_country_2,
        top1.browser
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.country AS session_country_2, c.browser
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    name,
    session_browser,
    session_country,
    session_country_2
FROM with_last
ORDER BY session_country DESC;
