WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        UPPER(device) device_upper,
        LENGTH(country) country_length
    FROM sessions
    WHERE device IN ('mobile', 'desktop')
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM base e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.started_at BETWEEN sub.start_date AND sub.end_date
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee, id, subscription_id) seq_no,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC, id, subscription_id) row_seq
    FROM matched
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id, subscription_id) AS prev_started_at
    FROM enriched
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY customer_id ORDER BY started_at, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
kept_keys AS (
    -- Set intersection on keys
    SELECT country, customer_id
    FROM sessions_sessions
    INTERSECT ALL
    SELECT country, customer_id
    FROM sessions
    WHERE country = 'TH'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions_sessions s
    INNER JOIN kept_keys k ON s.country = k.country AND s.customer_id = k.customer_id
)
SELECT
    customer_id,
    device,
    browser,
    started_at,
    country,
    device_upper,
    country_length,
    subscription_id,
    plan,
    CASE WHEN plan RLIKE '^(enterprise|premium)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS plan_zone
FROM verified
ORDER BY country_length;
