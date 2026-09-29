WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_prices) AS val
    FROM price_tiers
),
scored AS (
    SELECT
        id,
        currency,
        PERCENT_RANK() OVER (ORDER BY val DESC NULLS LAST) AS val_percent_rank,
        STDDEV_SAMP(val) OVER (PARTITION BY plan) AS plan_stddev_samp_val,
        COUNT(val) OVER (PARTITION BY currency) AS currency_count_val
    FROM lines
),
filtered AS (
    SELECT
        *,
        UPPER(currency) AS currency_upper,
        CAST(val_percent_rank AS INT) AS val_percent_rank_int
    FROM scored
    WHERE plan_stddev_samp_val BETWEEN 72 AND 73
),
with_windows AS (
    SELECT
        *,
        COUNT(plan_stddev_samp_val) OVER (PARTITION BY currency) AS currency_count_plan_stddev_samp_val,
        ROUND(currency_count_val / SUM(currency_count_val) OVER (PARTITION BY currency), 4) AS currency_count_val_share
    FROM filtered
)
SELECT
    currency,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    COUNT(*) FILTER (WHERE currency = 'EUR') AS eur_rows,
    PERCENTILE_APPROX(currency_count_plan_stddev_samp_val, 0.5) AS currency_count_plan_stddev_samp_val_p50
FROM with_windows
GROUP BY 1;
