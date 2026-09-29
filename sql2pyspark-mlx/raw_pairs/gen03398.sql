WITH lines AS (
    SELECT
        id,
        plan,
        currency,
        EXPLODE(discounts) AS val
    FROM price_tiers
)
SELECT
    CASE WHEN val >= 74 THEN 'PREMIUM' ELSE 'STANDARD' END AS val_tier,
    currency,
    MAX(val) FILTER (WHERE currency = 'USD') AS usd_max_val,
    SUM(val) FILTER (WHERE plan = 'basic') AS basic_total_val,
    SUM(CASE WHEN currency = 'EUR' THEN val ELSE 0 END) AS eur_val
FROM lines
GROUP BY ROLLUP (CASE WHEN val >= 74 THEN 'PREMIUM' ELSE 'STANDARD' END, currency)
ORDER BY eur_val DESC
