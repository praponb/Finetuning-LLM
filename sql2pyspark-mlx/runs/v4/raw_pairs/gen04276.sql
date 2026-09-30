WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_prices) AS val
    FROM price_tiers
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(val) OVER (PARTITION BY currency) AS currency_sum_val,
        MIN(val) OVER (PARTITION BY currency) AS currency_min_val,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val NULLS FIRST) AS val_percent_rank
    FROM exploded
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        currency,
        currency_sum_val,
        currency_min_val,
        COALESCE(val_percent_rank, 0) AS val_percent_rank_filled
    FROM scored
    WHERE effective_from >= TO_DATE('2024-06-02')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        currency,
        currency_sum_val,
        currency_min_val,
        val_percent_rank_filled,
        AVG(currency_min_val) OVER (PARTITION BY currency) AS currency_avg_currency_min_val,
        SUM(currency_sum_val) OVER (PARTITION BY currency) AS currency_sum_currency_sum_val
    FROM filtered
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        currency,
        currency_min_val,
        val_percent_rank_filled,
        currency_avg_currency_min_val,
        currency_sum_currency_sum_val,
        currency_sum_currency_sum_val - currency_avg_currency_min_val AS currency_sum_currency_sum_val_minus_currency_avg_currency_min_val
    FROM enriched
    WHERE currency_sum_val < 112
),
cleaned_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        currency,
        val_percent_rank_filled,
        currency_avg_currency_min_val,
        currency_sum_currency_sum_val,
        CAST(currency_avg_currency_min_val AS STRING) AS currency_avg_currency_min_val_num
    FROM cleaned
    WHERE currency_sum_currency_sum_val_minus_currency_avg_currency_min_val < 39
)
-- Grouped metrics
SELECT
    CASE WHEN currency_avg_currency_min_val >= 69 THEN 'LARGE' ELSE 'SMALL' END AS currency_avg_currency_min_val_tier,
    COUNT(*) FILTER (WHERE currency = 'EUR') AS eur_rows,
    SUM(currency_sum_currency_sum_val) AS total_currency_sum_currency_sum_val,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list
FROM cleaned_price_tiers
GROUP BY CASE WHEN currency_avg_currency_min_val >= 69 THEN 'LARGE' ELSE 'SMALL' END
