WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) e AS tier_value
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, currency
    FROM lines
    EXCEPT
    SELECT id, currency
    FROM price_tiers
    WHERE currency IN ('USD', 'THB', 'EUR')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    INNER JOIN kept_keys k ON s.id = k.id AND s.currency = k.currency
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        currency,
        tier_value,
        AVG(tier_value) OVER (PARTITION BY plan, currency) AS plan_avg_tier_value,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value) AS tier_value_dense_rank
    FROM verified
),
kept_keys_2 AS (
    -- Set intersection on keys
    SELECT currency
    FROM with_windows
    INTERSECT ALL
    SELECT currency
    FROM price_tiers
    WHERE plan = 'basic'
),
verified_price_tiers AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys_2 k ON s.currency = k.currency
)
-- Grouped metrics
SELECT
    CASE WHEN tier_value_dense_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END AS tier_value_dense_rank_tier,
    currency,
    SUM(CASE WHEN plan = 'pro' THEN tier_value ELSE 0 END) AS pro_tier_value,
    COUNT(CASE WHEN plan = 'starter' THEN 1 END) AS starter_count,
    ROUND(AVG(tier_value), 2) AS avg_tier_value
FROM verified_price_tiers
GROUP BY 1, 2
HAVING SUM(CASE WHEN plan = 'pro' THEN tier_value ELSE 0 END) < 0
