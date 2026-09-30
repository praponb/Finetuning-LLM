WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) li AS val
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val DESC) AS val_percent_rank,
        PERCENT_RANK() OVER (ORDER BY val NULLS FIRST) AS val_percent_rank_2,
        AVG(val) OVER (PARTITION BY currency) AS currency_avg_val,
        RANK() OVER (PARTITION BY currency ORDER BY val DESC) AS val_rank
    FROM exploded
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        effective_from,
        currency,
        val_percent_rank,
        val_percent_rank_2,
        val_rank,
        GREATEST(currency_avg_val, val_rank) AS greatest_currency_avg_val_val_rank,
        DATEDIFF(CURRENT_DATE(), effective_from) AS days_since_effective_from
    FROM metrics
    WHERE effective_from >= TO_DATE('2024-10-15')
      AND currency_avg_val >= 50
)
-- Grouped metrics
SELECT
    currency,
    COUNT_IF(currency = 'THB') AS thb_events,
    COUNT_IF(currency = 'USD') AS usd_events,
    COUNT(*) AS cnt,
    SUM(greatest_currency_avg_val_val_rank) FILTER (WHERE currency = 'THB') AS thb_total_greatest_currency_avg_val_val_rank
FROM cleaned
GROUP BY currency;
