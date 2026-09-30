WITH cleaned AS (
    SELECT
        id,
        name,
        email,
        country,
        city,
        credit_limit,
        age,
        SUBSTRING(email, 1, 3) email_prefix
    FROM customers
    WHERE country = 'DE'
),
kept AS (
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.customer_id = s.id
              AND x.store_id = 34
        )
),
latest_match AS (
    SELECT
        s.*,
        latest.session_country,
        latest.session_browser
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.country AS session_country, c.browser AS session_browser
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest AS (
    SELECT *
    FROM latest_match
    QUALIFY RANK() OVER (PARTITION BY city ORDER BY credit_limit DESC) = 1
),
with_last AS (
    SELECT
        s.id,
        s.country,
        s.credit_limit,
        s.age,
        s.session_browser,
        latest.session_started_at,
        latest.session_country_2
    FROM latest s
    JOIN LATERAL (
            SELECT c.started_at AS session_started_at, c.country AS session_country_2
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept_customers AS (
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.order_date >= s.session_started_at
        )
),
with_gaps AS (
    SELECT *, CASE WHEN session_started_at - LAG(session_started_at) OVER (PARTITION BY session_browser ORDER BY session_started_at, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM kept_customers
),
sessions_customers AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY session_browser ORDER BY session_started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
stats AS (
    SELECT
        CASE WHEN age >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_tier,
        CASE WHEN is_new_burst >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_new_burst_tier,
        COUNT_IF(session_browser = 'Safari') AS safari_events,
        MIN(age) AS min_age,
        MIN(session_started_at) AS started_at,
        MAX(session_started_at) AS last_seen
    FROM sessions_customers
    GROUP BY CASE WHEN age >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN is_new_burst >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    age_tier,
    is_new_burst_tier,
    min_age,
    started_at
FROM stats
ORDER BY age_tier DESC, min_age;
