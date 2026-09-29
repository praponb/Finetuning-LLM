WITH with_dims AS (
    -- Attach customers attributes
    SELECT
        s.*,
        c.name,
        c.segment,
        c.credit_limit
    FROM sessions s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM with_dims a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.started_at >= sub.start_date
       AND a.started_at < sub.end_date + INTERVAL 14 DAYS
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        name,
        credit_limit,
        subscription_id,
        plan,
        monthly_fee,
        credit_limit - subscription_id AS credit_limit_minus_subscription_id,
        DATE_TRUNC('WEEK', started_at) AS started_at_period
    FROM with_window
    WHERE plan IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY subscription_id NULLS LAST) AS subscription_id_rank,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee, id, subscription_id) AS event_seq,
        CUME_DIST() OVER (PARTITION BY plan, customer_id ORDER BY monthly_fee DESC NULLS LAST) AS monthly_fee_cume_dist
    FROM base
),
kept_keys AS (
    -- Set difference on keys
    SELECT customer_id, device
    FROM metrics
    EXCEPT ALL
    SELECT customer_id, device
    FROM sessions
    WHERE country IS NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.device = k.device
),
joined AS (
    -- Attach customers attributes
    SELECT src.*, p.segment
    FROM reconciled src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
summary AS (
    -- Grouped metrics
    SELECT
        device,
        SUM(subscription_id) FILTER (WHERE device = 'desktop') AS desktop_total_subscription_id,
        APPROX_PERCENTILE(monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS monthly_fee_quartiles,
        PERCENTILE_APPROX(credit_limit, 0.5) AS credit_limit_p50,
        COUNT(*) AS row_count,
        COUNT_IF(segment = 'Consumer') AS consumer_events,
        SORT_ARRAY(COLLECT_SET(segment)) AS segment_list
    FROM joined
    GROUP BY device
    HAVING desktop_total_subscription_id >= 0
)
SELECT
    device,
    desktop_total_subscription_id,
    monthly_fee_quartiles,
    credit_limit_p50,
    row_count
FROM summary;
