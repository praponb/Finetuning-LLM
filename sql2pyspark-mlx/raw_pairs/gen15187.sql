WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        SUBSTRING(country, 1, 4) AS country_prefix,
        DATE_FORMAT(started_at, 'yyyy-MM-dd') AS started_at_label
    FROM sessions
    WHERE started_at >= TO_TIMESTAMP('2023-03-30 12:00:00')
      AND started_at < '2023-11-01'
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM base e
    INNER JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.started_at >= sub.start_date
       AND e.started_at <= DATE_ADD(sub.end_date, 3)
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id, subscription_id) AS prev_started_at
    FROM matched
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY browser ORDER BY started_at, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM sessionized s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.page IN ('/checkout', '/cart')
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        started_at_label,
        subscription_id,
        monthly_fee,
        session_id,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY is_new_burst, id, subscription_id) AS row_seq,
        AVG(is_new_burst) OVER (PARTITION BY customer_id, device) AS customer_id_avg_is_new_burst,
        AVG(subscription_id) OVER (PARTITION BY device ORDER BY CAST(prev_started_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS subscription_id_avg_30d
    FROM not_flagged
)
SELECT
    id,
    browser,
    started_at,
    started_at_label,
    subscription_id,
    monthly_fee,
    session_id,
    subscription_id_avg_30d,
    CASE WHEN browser RLIKE '^(Edge|Firefox)$' THEN 'CORE' WHEN browser RLIKE '^(Chrome|Safari)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS browser_zone
FROM enriched
ORDER BY subscription_id DESC NULLS LAST
