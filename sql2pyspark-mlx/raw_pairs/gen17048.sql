WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) li AS val
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_FORMAT(effective_from, 'yyyy-MM') AS effective_from_label
    FROM lines
    WHERE effective_from >= '2025-07-01'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        effective_from,
        val,
        RANK() OVER (PARTITION BY MONTH(effective_from), currency ORDER BY val DESC NULLS LAST) AS val_rank,
        MAX(val) OVER (PARTITION BY currency) AS currency_max_val,
        SUM(val) OVER (PARTITION BY currency) AS currency_sum_val
    FROM filtered
),
filtered_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        effective_from,
        val,
        val_rank,
        currency_max_val,
        currency_sum_val,
        CASE WHEN currency_max_val >= 73 THEN 'High' WHEN currency_max_val >= 20 THEN 'Medium' ELSE 'Low' END AS currency_max_val_level,
        val_rank / NULLIF(currency_max_val, 0) AS val_rank_per_currency_max_val
    FROM with_windows
    WHERE effective_from > DATE '2024-05-06'
      AND currency_max_val BETWEEN 58 AND 63
)
SELECT
    id,
    effective_from,
    val,
    currency_max_val,
    currency_sum_val,
    currency_max_val_level,
    val_rank_per_currency_max_val
FROM filtered_price_tiers
ORDER BY val_rank_per_currency_max_val DESC;
