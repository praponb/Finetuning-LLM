WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(device)) device_title
    FROM sessions
    WHERE started_at > TIMESTAMP '2023-08-01 20:00:00'
      AND country IS DISTINCT FROM 'JP'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        device,
        started_at,
        country,
        device_title,
        TRIM(browser) clean_browser,
        COALESCE(country, 'DIRECT') resolved_country
    FROM filtered
    WHERE started_at > TO_TIMESTAMP('2023-03-14 17:00:00')
      AND customer_id IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        b.segment,
        b.signup_date,
        b.credit_limit
    FROM valid a
    LEFT JOIN customers b
        ON a.customer_id = b.id
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at ASC, id ASC) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM enriched
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY country ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
SELECT
    s.id,
    s.customer_id,
    s.device,
    s.started_at,
    s.country,
    s.device_title,
    s.clean_browser,
    s.segment,
    s.signup_date,
    (SELECT MAX(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS max_subscription_monthly_fee
FROM islands s
ORDER BY country ASC;
