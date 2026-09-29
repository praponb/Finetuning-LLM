WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(discounts) AS element
    FROM price_tiers
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(element) OVER (PARTITION BY currency) AS currency_max_element,
        PERCENT_RANK() OVER (PARTITION BY plan, currency ORDER BY element DESC NULLS FIRST) AS element_percent_rank,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY element NULLS LAST) AS element_dense_rank
    FROM lines
),
matched_keys AS (
    -- Set difference on keys
    SELECT plan, currency
    FROM scored
    EXCEPT ALL
    SELECT plan, currency
    FROM price_tiers
    WHERE plan = 'pro'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    JOIN matched_keys k ON s.plan = k.plan AND s.currency = k.currency
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        plan,
        currency,
        element,
        currency_max_element,
        element_percent_rank,
        element_dense_rank,
        DATE_SUB(effective_from, 38) AS effective_from_minus_38d
    FROM reconciled
    WHERE effective_from > DATE '2024-02-10'
),
scored_price_tiers AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(currency_max_element) OVER w_running AS plan_max_currency_max_element,
        AVG(element) OVER w_running AS plan_avg_element,
        PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('month', effective_from_minus_38d) ORDER BY currency_max_element) AS currency_max_element_percent_rank,
        PERCENT_RANK() OVER w_hist AS currency_max_element_percent_rank_2
    FROM valid
    WINDOW w_running AS (PARTITION BY plan, currency), w_hist AS (PARTITION BY plan ORDER BY currency_max_element DESC)
)
SELECT
    plan,
    element,
    currency_max_element,
    element_percent_rank,
    element_dense_rank,
    effective_from_minus_38d,
    plan_avg_element,
    currency_max_element_percent_rank,
    currency_max_element_percent_rank_2
FROM scored_price_tiers
ORDER BY currency_max_element DESC;
