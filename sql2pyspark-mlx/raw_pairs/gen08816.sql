WITH remaining_keys AS (
    SELECT id, country
    FROM sessions
    INTERSECT ALL
    SELECT id, country
    FROM sessions
    WHERE country IN ('DE', 'JP', 'UK')
),
verified AS (
    SELECT s.*
    FROM sessions s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.country = k.country
),
valid AS (
    SELECT
        *,
        SUBSTRING(country, 1, 4) AS country_prefix,
        MONTH(started_at) AS started_at_month
    FROM verified
    WHERE started_at > TIMESTAMP '2023-03-02 20:00:00'
),
joined AS (
    SELECT src.*, d.credit_limit
    FROM valid src
    INNER JOIN customers d
        ON src.customer_id = d.id
    WHERE d.credit_limit < 10700
),
eligible AS (
    SELECT s.*
    FROM joined s
    WHERE EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time < s.started_at
        )
),
with_dims AS (
    SELECT
        src.*,
        c.credit_limit AS customer_credit_limit,
        c.segment
    FROM eligible src
    INNER JOIN customers c
        ON src.customer_id = c.id
),
filtered AS (
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        started_at_month,
        credit_limit,
        segment,
        HOUR(started_at) AS started_at_hour,
        CAST(customer_credit_limit AS INT) AS customer_credit_limit_int
    FROM with_dims
    WHERE started_at >= DATE '2023-06-03'
      AND device IN ('tablet', 'mobile')
),
eligible_sessions AS (
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.page = '/home'
        )
)
SELECT
    s.id,
    s.customer_id,
    s.browser,
    s.country,
    s.credit_limit,
    s.segment,
    s.started_at_hour,
    s.customer_credit_limit_int,
    CASE WHEN country RLIKE '^(SG|UK)$' THEN 'PRIORITY' WHEN country RLIKE '^(TH|US)$' THEN 'CORE' ELSE 'GROUP_B' END AS country_zone,
    (SELECT MIN(o2.discount) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS min_order_discount
FROM eligible_sessions s
ORDER BY browser
