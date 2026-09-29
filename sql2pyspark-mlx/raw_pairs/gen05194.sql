WITH flattened AS (
    SELECT
        id,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) e AS tier_value
),
remaining_keys AS (
    SELECT currency, id
    FROM flattened
    EXCEPT ALL
    SELECT currency, id
    FROM price_tiers
    WHERE plan IN ('starter', 'pro', 'enterprise')
),
reconciled AS (
    SELECT s.*
    FROM flattened s
    INNER JOIN remaining_keys k ON s.currency = k.currency AND s.id = k.id
),
metrics AS (
    SELECT
        *,
        STDDEV_SAMP(tier_value) OVER (PARTITION BY currency) AS currency_stddev_samp_tier_value,
        RANK() OVER (ORDER BY tier_value NULLS LAST) AS tier_value_rank
    FROM reconciled
),
valid AS (
    SELECT *, FLOOR(tier_value) AS tier_value_whole
    FROM metrics
    WHERE tier_value <= 40
),
with_windows AS (
    SELECT
        id,
        currency,
        currency_stddev_samp_tier_value,
        DENSE_RANK() OVER (ORDER BY currency_stddev_samp_tier_value DESC NULLS FIRST) AS currency_stddev_samp_tier_value_dense_rank,
        MIN(tier_value) OVER (PARTITION BY currency) AS currency_min_tier_value,
        RANK() OVER (PARTITION BY currency ORDER BY currency_stddev_samp_tier_value DESC) AS currency_stddev_samp_tier_value_rank,
        SUM(tier_value) OVER (PARTITION BY currency) AS currency_sum_tier_value
    FROM valid
    WHERE tier_value_rank <= 8
),
with_windows_price_tiers AS (
    SELECT *, PERCENT_RANK() OVER (ORDER BY currency_stddev_samp_tier_value DESC) AS currency_stddev_samp_tier_value_percent_rank
    FROM with_windows
),
cleaned AS (
    SELECT
        currency_stddev_samp_tier_value,
        currency_stddev_samp_tier_value_dense_rank,
        currency_min_tier_value,
        currency_stddev_samp_tier_value_rank,
        currency_sum_tier_value,
        currency_stddev_samp_tier_value_percent_rank,
        ROUND(currency_sum_tier_value, 1) AS currency_sum_tier_value_rounded
    FROM with_windows_price_tiers
    WHERE currency_stddev_samp_tier_value = 39
)
SELECT
    CASE WHEN currency_min_tier_value >= 85 THEN 'LARGE' ELSE 'SMALL' END AS currency_min_tier_value_tier,
    CASE WHEN currency_sum_tier_value_rounded >= 90 THEN 'LARGE' ELSE 'SMALL' END AS currency_sum_tier_value_rounded_tier,
    MAX(currency_sum_tier_value_rounded) AS max_currency_sum_tier_value_rounded,
    APPROX_PERCENTILE(currency_stddev_samp_tier_value_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS currency_stddev_samp_tier_value_percent_rank_quartiles,
    COUNT(*) AS num_rows,
    ROUND(AVG(currency_sum_tier_value_rounded), 2) AS avg_currency_sum_tier_value_rounded,
    PERCENTILE_APPROX(currency_stddev_samp_tier_value_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS currency_stddev_samp_tier_value_dense_rank_quartiles
FROM cleaned
GROUP BY CASE WHEN currency_min_tier_value >= 85 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN currency_sum_tier_value_rounded >= 90 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY max_currency_sum_tier_value_rounded DESC NULLS FIRST
