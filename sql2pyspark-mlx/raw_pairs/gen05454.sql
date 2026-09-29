WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        EXPLODE_OUTER(tier_prices) AS element
    FROM price_tiers
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, plan
    FROM lines
    EXCEPT DISTINCT
    SELECT id, plan
    FROM price_tiers
    WHERE currency IN ('THB', 'EUR')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    INNER JOIN kept_keys k ON s.id = k.id AND s.plan = k.plan
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY plan ORDER BY element DESC) AS element_rank,
        RANK() OVER (PARTITION BY plan ORDER BY element DESC) AS element_rank_2,
        STDDEV_SAMP(element) OVER (PARTITION BY plan) AS plan_stddev_samp_element,
        COUNT(element) OVER (PARTITION BY plan) AS plan_count_element
    FROM kept_rows
    WHERE plan != 'enterprise'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        element_rank_2,
        plan_count_element,
        element_rank_2 - plan_stddev_samp_element AS element_rank_2_minus_plan_stddev_samp_element
    FROM enriched
    WHERE plan_stddev_samp_element BETWEEN 60 AND 65
),
kept_keys_2 AS (
    -- Set difference on keys
    SELECT id
    FROM valid
    EXCEPT
    SELECT id
    FROM price_tiers
    WHERE currency != 'EUR'
),
kept_rows_price_tiers AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys_2 k ON s.id = k.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN element_rank_2_minus_plan_stddev_samp_element >= 73 THEN 'LARGE' ELSE 'SMALL' END AS element_rank_2_minus_plan_stddev_samp_element_tier,
    SUM(CASE WHEN plan = 'pro' THEN element_rank_2_minus_plan_stddev_samp_element ELSE 0 END) AS pro_element_rank_2_minus_plan_stddev_samp_element,
    COUNT_IF(plan = 'pro') AS pro_events,
    AVG(element_rank_2) AS avg_element_rank_2,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    COUNT(*) FILTER (WHERE plan = 'enterprise') AS enterprise_rows,
    SIZE(COLLECT_SET(plan)) AS num_plans
FROM kept_rows_price_tiers
GROUP BY CASE WHEN element_rank_2_minus_plan_stddev_samp_element >= 73 THEN 'LARGE' ELSE 'SMALL' END;
