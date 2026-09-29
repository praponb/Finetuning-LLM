WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE(tier_prices) AS element
    FROM price_tiers
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(currency) AS currency_upper
    FROM exploded
    WHERE MONTH(effective_from) IN (7, 9, 10)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        effective_from,
        currency,
        element,
        currency_upper,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY element DESC) AS element_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('week', effective_from) ORDER BY element NULLS FIRST) AS element_percent_rank_2,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY element DESC NULLS LAST) AS element_cume_dist,
        DENSE_RANK() OVER (PARTITION BY YEAR(effective_from), currency ORDER BY element DESC) AS element_dense_rank
    FROM cleaned
),
enriched AS (
    -- Ranking and gap features
    SELECT
        effective_from,
        currency,
        element,
        element_percent_rank,
        element_percent_rank_2,
        PERCENT_RANK() OVER (PARTITION BY MONTH(effective_from), currency ORDER BY element DESC NULLS LAST) AS element_percent_rank_3,
        MAX(element_dense_rank) OVER (PARTITION BY currency) AS currency_max_element_dense_rank
    FROM with_windows
    WHERE effective_from < '2024-11-01'
),
daily AS (
    -- Grouped metrics
    SELECT
        currency,
        SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
        COUNT(*) FILTER (WHERE currency = 'THB') AS thb_rows,
        SORT_ARRAY(COLLECT_SET(currency)) AS currency_list_2,
        ROUND(SUM(element) / NULLIF(COUNT(CASE WHEN currency = 'USD' THEN 1 END), 0), 2) AS element_per_usd,
        COUNT(*) AS row_count,
        ROUND(AVG(element_percent_rank_2), 2) AS avg_element_percent_rank_2
    FROM enriched
    GROUP BY 1
)
SELECT
    currency,
    thb_rows,
    element_per_usd,
    row_count,
    avg_element_percent_rank_2,
    CASE WHEN currency RLIKE '^(THB|EUR)$' THEN 'OTHER_ZONE' WHEN currency RLIKE '^(USD)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS currency_zone
FROM daily
ORDER BY thb_rows DESC
