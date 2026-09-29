WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM customers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY credit_limit NULLS LAST, id) = 1
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN city IN ('Chiang Mai', 'Bangkok') THEN 'Core' ELSE 'Other' END AS city_group,
        COALESCE(city, 'N/A') AS city_clean
    FROM deduped
    WHERE segment IS NOT NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND ch.opened_date < s.signup_date
        )
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM kept
    QUALIFY RANK() OVER (PARTITION BY city, segment ORDER BY credit_limit) = 1
),
latest_match AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.browser,
        x.started_at
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.browser, c.started_at
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.signup_date
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts x
            WHERE x.customer_id = s.id
              AND x.opened_date < s.signup_date
        )
)
SELECT
    id,
    email,
    country,
    city,
    segment,
    signup_date,
    credit_limit,
    age,
    city_group
FROM not_flagged
ORDER BY id DESC NULLS FIRST;
