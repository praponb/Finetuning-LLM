WITH flattened AS (
    SELECT
        id,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) t AS element
),
metrics AS (
    SELECT
        *,
        SUM(element) OVER (PARTITION BY currency) AS currency_sum_element,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY element NULLS FIRST) AS element_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY element NULLS FIRST) AS element_percent_rank_2,
        CUME_DIST() OVER (ORDER BY element NULLS LAST) AS element_cume_dist,
        SUM(element) OVER (PARTITION BY currency) AS currency_sum_element_2
    FROM flattened
),
filtered AS (
    SELECT
        effective_from,
        currency,
        currency_sum_element,
        element_cume_dist,
        currency_sum_element_2,
        CASE WHEN currency = 'THB' THEN 1 ELSE 0 END AS is_thb
    FROM metrics
    WHERE effective_from >= DATE '2024-03-19'
),
filtered_price_tiers AS (
    SELECT
        *,
        ROUND(is_thb, 0) AS is_thb_rounded,
        currency_sum_element_2 + currency_sum_element AS currency_sum_element_2_plus_currency_sum_element
    FROM filtered
    WHERE effective_from >= DATE '2024-03-12'
      AND currency_sum_element_2 BETWEEN 207 AND 249
),
metrics_price_tiers AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY currency_sum_element_2_plus_currency_sum_element NULLS FIRST) AS currency_sum_element_2_plus_currency_sum_element_dense_rank,
        COUNT(currency_sum_element) OVER (PARTITION BY currency) AS currency_count_currency_sum_element
    FROM filtered_price_tiers
),
scored AS (
    SELECT
        effective_from,
        currency,
        currency_sum_element,
        element_cume_dist,
        currency_sum_element_2,
        is_thb,
        is_thb_rounded,
        currency_sum_element_2_plus_currency_sum_element,
        currency_sum_element_2_plus_currency_sum_element_dense_rank,
        currency_count_currency_sum_element,
        DENSE_RANK() OVER (ORDER BY currency_sum_element_2_plus_currency_sum_element_dense_rank NULLS LAST) AS currency_sum_element_2_plus_currency_sum_element_dense_rank_dense_rank,
        PERCENT_RANK() OVER (ORDER BY currency_sum_element NULLS LAST) AS currency_sum_element_percent_rank
    FROM metrics_price_tiers
)
SELECT *
FROM (
    SELECT
        currency,
        currency_sum_element_percent_rank,
        is_thb_rounded,
        currency_sum_element_2_plus_currency_sum_element,
        element_cume_dist
    FROM scored
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric_name IN (currency_sum_element_percent_rank, is_thb_rounded, currency_sum_element_2_plus_currency_sum_element, element_cume_dist)
);
