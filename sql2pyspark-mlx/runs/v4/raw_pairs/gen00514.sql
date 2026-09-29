WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY device, customer_id ORDER BY started_at DESC, id) AS event_seq,
        LAST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS event_seq_2,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at, id) AS seq_no
    FROM sessions
),
matched_keys AS (
    -- Set difference on keys
    SELECT customer_id, browser
    FROM enriched
    EXCEPT
    SELECT customer_id, browser
    FROM sessions
    WHERE country IN ('TH', 'DE', 'SG')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id AND s.browser = k.browser
),
kept_keys AS (
    -- Set intersection on keys
    SELECT customer_id, device
    FROM verified
    INTERSECT ALL
    SELECT customer_id, device
    FROM sessions
    WHERE customer_id = 11
),
verified_sessions AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.device = k.device
)
SELECT
    s.id,
    s.customer_id,
    s.browser,
    s.started_at,
    s.country,
    s.event_seq,
    (SELECT AVG(s2.monthly_fee) FROM subscriptions s2 WHERE s2.customer_id = s.customer_id) AS avg_subscription_monthly_fee
FROM verified_sessions s
ORDER BY browser NULLS LAST;
