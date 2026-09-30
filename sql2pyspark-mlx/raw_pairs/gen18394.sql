WITH joined AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.started_at,
        s.country,
        p.city
    FROM sessions s
    LEFT JOIN customers p
        ON s.customer_id = p.id
    WHERE NOT p.is_active
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS('-', country, city) AS country_city_key,
        COALESCE(country, 'DIRECT') AS country_clean
    FROM joined
    WHERE started_at > TIMESTAMP '2023-03-17 23:00:00'
      AND started_at >= '2024-08-05'
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM valid s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.started_at >= sub.start_date
       AND s.started_at <= DATE_ADD(sub.end_date, 3)
),
kept_keys AS (
    -- Set difference on keys
    SELECT customer_id, id
    FROM matched
    EXCEPT ALL
    SELECT customer_id, id
    FROM sessions
    WHERE device IN ('desktop', 'mobile', 'tablet')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM matched s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.segment,
        p.country AS customer_country
    FROM verified e
    LEFT JOIN customers p
        ON e.customer_id = p.id
),
valid_sessions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(country) AS country_length,
        LOWER(customer_country) AS customer_country_lower
    FROM with_dims
    WHERE plan IN ('premium', 'enterprise', 'standard')
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid_sessions s
    WHERE EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.duration_sec > 340
        )
)
-- Join a dimension and aggregate
SELECT
    p.country AS customer_country,
    p.segment AS customer_segment,
    e.city AS session_city,
    COUNT(*) AS num_rows,
    SUM(e.subscription_id) AS total_subscription_id,
    MAX(e.monthly_fee) AS max_monthly_fee,
    GROUPING_ID() AS grouping_level
FROM eligible e
INNER JOIN customers p ON e.customer_id = p.id
GROUP BY GROUPING SETS ((p.country, p.segment, e.city), (p.country, p.segment), (p.country), ())
