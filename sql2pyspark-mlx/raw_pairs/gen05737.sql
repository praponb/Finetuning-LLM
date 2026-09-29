WITH matched_keys AS (
    -- Set difference on keys
    SELECT promo_code, id
    FROM promotions
    EXCEPT
    SELECT promo_code, id
    FROM promotions
    WHERE discount_pct > 0.12
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM promotions s
    JOIN matched_keys k ON s.promo_code = k.promo_code AND s.id = k.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN discount_pct >= 0.35 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_pct_tier,
    DATE_TRUNC('WEEK', ends_on) AS week_start,
    STDDEV_SAMP(discount_pct) AS discount_pct_stddev,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes,
    COUNT(*) AS event_count,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes_2
FROM reconciled
GROUP BY 1, 2;
