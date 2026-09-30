WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(discounts) AS element
    FROM price_tiers
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(element) OVER (PARTITION BY plan) plan_count_element,
        RANK() OVER (PARTITION BY plan ORDER BY element) element_rank,
        PERCENT_RANK() OVER (ORDER BY element DESC) element_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY element DESC NULLS LAST) element_percent_rank_2,
        SUM(element) OVER (PARTITION BY currency) currency_sum_element
    FROM flattened
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        effective_from,
        currency,
        element,
        plan_count_element,
        element_rank,
        element_percent_rank,
        currency_sum_element,
        CASE WHEN element_percent_rank >= 0.78 THEN 'High' WHEN element_percent_rank >= 0.26 THEN 'Medium' ELSE 'Low' END element_percent_rank_band,
        element_percent_rank_2 * element_percent_rank element_percent_rank_2_times_element_percent_rank
    FROM with_windows
    WHERE element_percent_rank_2 <= 0.29
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        effective_from,
        currency,
        element,
        plan_count_element,
        element_rank,
        element_percent_rank,
        currency_sum_element,
        element_percent_rank_band,
        element_percent_rank_2_times_element_percent_rank,
        RANK() OVER (ORDER BY element_percent_rank_2_times_element_percent_rank DESC NULLS FIRST) element_percent_rank_2_times_element_percent_rank_rank,
        PERCENT_RANK() OVER (ORDER BY element DESC) element_percent_rank_2
    FROM cleaned
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN currency_sum_element >= 324 THEN 'HIGH' ELSE 'LOW' END AS currency_sum_element_tier,
        CASE WHEN element_percent_rank_2_times_element_percent_rank >= 79 THEN 'LARGE' ELSE 'SMALL' END AS element_percent_rank_2_times_element_percent_rank_tier,
        ROUND(SUM(element_percent_rank_2_times_element_percent_rank) / NULLIF(COUNT(CASE WHEN currency = 'USD' THEN 1 END), 0), 2) AS element_percent_rank_2_times_element_percent_rank_per_usd,
        COUNT(*) FILTER (WHERE currency = 'USD') AS usd_rows,
        SUM(CASE WHEN currency = 'USD' THEN currency_sum_element ELSE 0 END) AS usd_currency_sum_element,
        ROUND(SUM(element) / NULLIF(COUNT(CASE WHEN currency = 'THB' THEN 1 END), 0), 2) AS element_per_thb,
        AVG(currency_sum_element) FILTER (WHERE currency = 'USD') AS usd_avg_currency_sum_element,
        COUNT(*) FILTER (WHERE currency = 'THB') AS thb_rows
    FROM enriched
    GROUP BY CASE WHEN currency_sum_element >= 324 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN element_percent_rank_2_times_element_percent_rank >= 79 THEN 'LARGE' ELSE 'SMALL' END
    HAVING usd_rows < 1
)
SELECT
    currency_sum_element_tier,
    element_percent_rank_2_times_element_percent_rank_tier,
    element_percent_rank_2_times_element_percent_rank_per_usd,
    usd_rows,
    usd_currency_sum_element,
    element_per_thb,
    usd_avg_currency_sum_element,
    thb_rows
FROM grouped
ORDER BY thb_rows DESC
