WITH base AS (
    SELECT *, LOWER(device) AS device_lower
    FROM sessions
    WHERE started_at > DATE '2023-06-20'
),
not_flagged AS (
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time >= s.started_at
        )
),
eligible AS (
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time >= s.started_at
        )
),
matched AS (
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM eligible s
    JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.started_at BETWEEN sub.start_date AND sub.end_date
)
SELECT
    c.segment,
    c.city,
    e.browser,
    COUNT(*) AS cnt,
    MIN(e.subscription_id) AS min_subscription_id,
    AVG(e.monthly_fee) AS avg_monthly_fee
FROM matched e
JOIN customers c ON e.customer_id = c.id
GROUP BY c.segment, c.city, e.browser;
