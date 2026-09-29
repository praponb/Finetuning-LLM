WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(browser) AS browser_upper
    FROM sessions
    WHERE device NOT IN ('mobile', 'tablet')
      AND started_at > TIMESTAMP '2023-04-06 12:00:00'
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.browser,
        a.country,
        p.signup_date,
        p.country AS customer_country,
        p.credit_limit
    FROM valid a
    LEFT JOIN customers p
        ON a.customer_id <=> p.id
    WHERE p.age <> 47
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM joined s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.signup_date BETWEEN sub.start_date AND sub.end_date
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(subscription_id) OVER (PARTITION BY customer_country) AS customer_country_min_subscription_id,
        ROW_NUMBER() OVER (PARTITION BY country, customer_country ORDER BY signup_date DESC, id, subscription_id) AS seq_no,
        LAST_VALUE(country) IGNORE NULLS OVER (PARTITION BY country ORDER BY monthly_fee, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country,
        LEAD(credit_limit, 2, 0) OVER (PARTITION BY country ORDER BY monthly_fee, id, subscription_id) AS next_credit_limit,
        SUM(subscription_id) OVER (ORDER BY monthly_fee, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_subscription_id
    FROM matched
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.page,
        latest.duration_sec
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.page, c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.signup_date
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Join a dimension and aggregate
SELECT
    c.city,
    a.plan,
    COUNT(*) AS rows_total,
    MIN(a.duration_sec) AS min_duration_sec,
    MAX(a.seq_no) AS max_seq_no
FROM with_last a
LEFT JOIN customers c ON a.customer_id = c.id
GROUP BY CUBE (c.city, a.plan)
