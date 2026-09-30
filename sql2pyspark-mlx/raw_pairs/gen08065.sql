WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) e AS element
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT *, DENSE_RANK() OVER (PARTITION BY currency, plan ORDER BY element ASC NULLS FIRST) AS element_dense_rank
    FROM exploded
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, element_dense_rank + element AS element_dense_rank_plus_element
    FROM enriched
    WHERE currency NOT IN ('THB', 'EUR')
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('-', plan, currency) AS plan_currency_key,
        TRIM(plan) AS clean_plan
    FROM valid
    WHERE element_dense_rank_plus_element <= 68
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY element ASC) AS element_rank,
        RANK() OVER (PARTITION BY plan ORDER BY element_dense_rank_plus_element ASC NULLS LAST) AS element_dense_rank_plus_element_rank,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY element_dense_rank DESC NULLS FIRST) AS element_dense_rank_dense_rank,
        MIN(element) OVER (PARTITION BY plan, currency) AS plan_min_element,
        DENSE_RANK() OVER (ORDER BY element DESC) AS element_dense_rank_2
    FROM cleaned
    WHERE plan IN ('enterprise', 'starter', 'pro')
),
enriched_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_grp AS element_dense_rank_plus_element_rank_2,
        ROUND(element_dense_rank / SUM(element_dense_rank) OVER (PARTITION BY currency, plan), 4) AS element_dense_rank_share,
        CUME_DIST() OVER (ORDER BY plan_min_element ASC NULLS FIRST) AS plan_min_element_cume_dist,
        AVG(element_dense_rank_plus_element) OVER (PARTITION BY currency) AS currency_avg_element_dense_rank_plus_element,
        RANK() OVER (PARTITION BY currency, plan ORDER BY element_rank ASC NULLS LAST) AS element_rank_rank
    FROM metrics
    WINDOW w_grp AS (PARTITION BY plan, currency ORDER BY element_dense_rank_plus_element DESC NULLS LAST)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        currency,
        element_dense_rank,
        element_dense_rank_plus_element,
        plan_currency_key,
        element_dense_rank_plus_element_rank,
        plan_min_element,
        element_dense_rank_2,
        plan_min_element_cume_dist,
        MIN(element_dense_rank_plus_element) OVER w_part AS plan_min_element_dense_rank_plus_element
    FROM enriched_price_tiers
    WINDOW w_part AS (PARTITION BY plan)
)
SELECT
    id,
    currency,
    element_dense_rank,
    element_dense_rank_plus_element,
    element_dense_rank_plus_element_rank,
    plan_min_element,
    element_dense_rank_2,
    plan_min_element_cume_dist,
    plan_min_element_dense_rank_plus_element,
    CASE WHEN currency RLIKE '^(THB|USD)$' THEN 'CORE' ELSE 'PRIORITY' END AS currency_zone
FROM with_windows
ORDER BY element_dense_rank_2 DESC NULLS FIRST;
