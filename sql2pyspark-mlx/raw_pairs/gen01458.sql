WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) li AS val
),
scored AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY val DESC NULLS FIRST) val_cume_dist,
        STDDEV_SAMP(val) OVER (PARTITION BY MONTH(effective_from), plan) effective_from_month_stddev_samp_val,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY val DESC) val_cume_dist_2,
        RANK() OVER (ORDER BY val) val_rank
    FROM flattened
    WHERE currency IN ('EUR', 'USD', 'THB')
),
remaining_keys AS (
    SELECT currency, plan
    FROM scored
    INTERSECT
    SELECT currency, plan
    FROM price_tiers
    WHERE effective_from >= '2024-07-06'
),
verified AS (
    SELECT s.*
    FROM scored s
    INNER JOIN remaining_keys k ON s.currency = k.currency AND s.plan = k.plan
),
valid AS (
    SELECT
        *,
        INITCAP(TRIM(currency)) currency_title,
        val_rank * effective_from_month_stddev_samp_val val_rank_times_effective_from_month_stddev_samp_val
    FROM verified
    WHERE effective_from >= DATE '2024-01-27'
)
SELECT
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    DATE_TRUNC('YEAR', effective_from) AS year_start,
    COUNT(DISTINCT currency) AS distinct_currencies,
    PERCENTILE_APPROX(val_cume_dist_2, ARRAY(0.25, 0.5, 0.75)) AS val_cume_dist_2_quartiles,
    APPROX_PERCENTILE(val_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS val_cume_dist_quartiles,
    COUNT(CASE WHEN currency = 'USD' THEN 1 END) AS usd_count
FROM valid
GROUP BY 1, 2
HAVING usd_count >= 1;
