WITH exploded AS (
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_prices) AS element
    FROM price_tiers
)
SELECT
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    SUM(CASE WHEN currency = 'EUR' THEN element ELSE 0 END) AS eur_element,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    SUM(CASE WHEN currency = 'USD' THEN element ELSE 0 END) AS usd_element
FROM exploded
GROUP BY quarter_start;
