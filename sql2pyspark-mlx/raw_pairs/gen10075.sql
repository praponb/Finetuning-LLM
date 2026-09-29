WITH base AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        LOWER(browser) browser_lower
    FROM sessions
    WHERE started_at < '2025-09-01'
),
joined AS (
    SELECT
        s.id,
        s.customer_id,
        s.device,
        s.browser,
        s.browser_lower,
        d.credit_limit,
        d.age,
        d.signup_date
    FROM base s
    JOIN customers d
        ON s.customer_id = d.id
    WHERE d.age <= 66
),
kept AS (
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time < s.signup_date
        )
),
deduped AS (
    SELECT
        id,
        customer_id,
        device,
        browser_lower,
        credit_limit,
        age,
        signup_date
    FROM kept
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY credit_limit NULLS FIRST, id) = 1
)
SELECT
    id,
    age,
    signup_date
FROM deduped;
