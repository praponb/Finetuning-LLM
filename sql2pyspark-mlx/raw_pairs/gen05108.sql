WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_limits) AS tier_value
    FROM price_tiers
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value,
        AVG(tier_value) OVER w_grp AS currency_avg_tier_value,
        PERCENT_RANK() OVER (ORDER BY tier_value DESC NULLS LAST) AS tier_value_percent_rank
    FROM lines
    WINDOW w_grp AS (PARTITION BY currency)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(plan)) AS plan_title,
        CONCAT_WS('/', currency, plan) AS currency_plan_key
    FROM metrics
    WHERE effective_from > DATE '2024-04-24'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN plan IN ('basic', 'starter') THEN 'Group A' ELSE 'Other' END AS plan_group,
        DATEDIFF(CURRENT_DATE(), effective_from) / 30 AS months_since_effective_from
    FROM valid
    WHERE effective_from > TO_DATE('2024-02-18')
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, plan
    FROM base
    INTERSECT ALL
    SELECT id, plan
    FROM price_tiers
    WHERE effective_from BETWEEN '2023-05-29' AND '2024-10-11'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN matched_keys k ON s.id = k.id AND s.plan = k.plan
)
SELECT
    currency,
    tier_value_percent_rank,
    plan_title,
    currency_plan_key,
    months_since_effective_from
FROM reconciled
ORDER BY months_since_effective_from;
