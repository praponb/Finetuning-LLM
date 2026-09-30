WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        city,
        segment,
        signup_date,
        is_active,
        age,
        CAST(is_active AS INT) AS is_active_flag,
        CONCAT(city, ' - ', email) AS city_email
    FROM customers
    WHERE signup_date > TO_DATE('2023-08-30')
      AND segment IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY is_active_flag DESC NULLS LAST, id) AS rank_in_group
        FROM filtered
    ) x
    WHERE rank_in_group = 1
),
with_latest AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.session_browser
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.browser AS session_browser
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.signup_date
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.country = 'JP'
        )
)
SELECT
    id,
    email,
    city,
    signup_date,
    age,
    is_active_flag,
    city_email,
    rank_in_group
FROM not_flagged
