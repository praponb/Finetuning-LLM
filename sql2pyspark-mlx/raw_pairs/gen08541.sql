WITH enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.city,
        d.age,
        d.segment
    FROM subscriptions s
    LEFT JOIN customers d
        ON s.customer_id = d.id
    WHERE d.country <> 'JP'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        status,
        age,
        segment
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY end_date, id) = 1
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.end_date,
        a.age,
        c.name,
        c.signup_date,
        c.country
    FROM latest a
    INNER JOIN customers c
        ON a.customer_id = c.id
),
joined_subscriptions AS (
    -- Attach customers attributes
    SELECT a.*, b.signup_date AS customer_signup_date
    FROM joined a
    LEFT JOIN customers b
        ON a.customer_id = b.id
),
enriched_subscriptions AS (
    -- Attach customers attributes
    SELECT
        src.*,
        d.segment,
        d.age AS customer_age
    FROM joined_subscriptions src
    INNER JOIN customers d
        ON src.customer_id = d.id
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM enriched_subscriptions a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.customer_signup_date >= sub.start_date
       AND a.customer_signup_date <= DATE_ADD(sub.end_date, 7)
),
totals AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('YEAR', customer_signup_date) AS year_start,
        DATE_TRUNC('QUARTER', end_date) AS quarter_start,
        AVG(customer_age) FILTER (WHERE plan = 'premium') AS premium_avg_customer_age,
        COUNT_IF(plan = 'standard') AS standard_events
    FROM matched
    GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', customer_signup_date), DATE_TRUNC('QUARTER', end_date)), (DATE_TRUNC('YEAR', customer_signup_date)), ())
)
SELECT premium_avg_customer_age, standard_events
FROM totals;
