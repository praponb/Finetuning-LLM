WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) x AS element
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(element) OVER w_part AS currency_sum_element,
        CUME_DIST() OVER w_part_2 AS element_cume_dist,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY element DESC NULLS LAST) AS element_dense_rank
    FROM lines
    WINDOW w_part AS (PARTITION BY currency), w_part_2 AS (ORDER BY element)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(currency_sum_element) OVER w_grp AS currency_sum_currency_sum_element,
        PERCENT_RANK() OVER (ORDER BY element_cume_dist NULLS LAST) AS element_cume_dist_percent_rank,
        PERCENT_RANK() OVER (ORDER BY element_cume_dist) AS element_cume_dist_percent_rank_2
    FROM enriched
    WHERE element_dense_rank BETWEEN 3 AND 7
    WINDOW w_grp AS (PARTITION BY currency)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY currency_sum_currency_sum_element DESC NULLS FIRST) AS currency_sum_currency_sum_element_dense_rank,
        AVG(element) OVER (PARTITION BY plan) AS plan_avg_element,
        DENSE_RANK() OVER (PARTITION BY plan, currency ORDER BY element DESC NULLS LAST) AS element_dense_rank_2,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY currency_sum_currency_sum_element DESC NULLS LAST) AS currency_sum_currency_sum_element_dense_rank_2,
        COUNT(element_cume_dist_percent_rank_2) OVER (PARTITION BY currency) AS currency_count_element_cume_dist_percent_rank_2
    FROM metrics
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, GREATEST(element_cume_dist, currency_sum_element) AS greatest_element_cume_dist_currency_sum_element
    FROM scored
    WHERE currency IN ('USD', 'EUR', 'THB')
),
kept_keys AS (
    -- Set intersection on keys
    SELECT currency, plan
    FROM filtered
    INTERSECT ALL
    SELECT currency, plan
    FROM price_tiers
    WHERE currency = 'USD'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.currency = k.currency AND s.plan = k.plan
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        plan,
        greatest_element_cume_dist_currency_sum_element,
        element_cume_dist,
        element_cume_dist_percent_rank_2,
        element_cume_dist_percent_rank
    FROM verified
) src
UNPIVOT (
    value FOR metric_name IN (greatest_element_cume_dist_currency_sum_element, element_cume_dist, element_cume_dist_percent_rank_2 AS ELEMENT_CUME_DIST_PERCENT_RANK_2_VAL, element_cume_dist_percent_rank AS ELEMENT_CUME_DIST_PERCENT_RANK_VAL)
) unp
