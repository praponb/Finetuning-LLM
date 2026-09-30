WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        FIRST_VALUE(monthly_fee) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee,
        SUM(monthly_fee) OVER (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee
    FROM subscriptions
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY customer_id, plan ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id), 4) AS monthly_fee_share
    FROM enriched
),
with_dims AS (
    -- Attach customers attributes
    SELECT src.*, d.city
    FROM metrics src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT plan
    FROM with_dims
    EXCEPT
    SELECT plan
    FROM subscriptions
    WHERE YEAR(end_date) = 2025
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    JOIN matched_keys k ON s.plan = k.plan
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_FORMAT(start_date, 'yyyy-MM-dd') AS start_date_label,
        ADD_MONTHS(end_date, 12) AS end_date_plus_months
    FROM verified
    WHERE first_known_monthly_fee <= 80
),
with_dims_subscriptions AS (
    -- Attach customers attributes
    SELECT src.*, b.signup_date
    FROM base src
    LEFT JOIN customers b
        ON src.customer_id = b.id
),
enriched_subscriptions AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.city AS customer_city,
        d.signup_date AS customer_signup_date
    FROM with_dims_subscriptions s
    LEFT JOIN customers d
        ON s.customer_id = d.id
)
-- Aggregate with conditional metrics
SELECT
    city,
    DATE_TRUNC('YEAR', end_date_plus_months) AS year_start,
    SUM(CASE WHEN customer_city = 'New York' THEN monthly_fee_share ELSE 0 END) AS new_york_monthly_fee_share,
    APPROX_PERCENTILE(running_monthly_fee, 0.5) AS running_monthly_fee_p50,
    SIZE(COLLECT_SET(customer_city)) AS num_customer_cities,
    PERCENT_RANK() OVER (PARTITION BY city ORDER BY APPROX_PERCENTILE(running_monthly_fee, 0.5) DESC) AS running_monthly_fee_p50_percent_rank
FROM enriched_subscriptions
GROUP BY 1, 2
