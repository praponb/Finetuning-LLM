WITH exploded AS (
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE(tier_prices) AS element
    FROM price_tiers
)
SELECT
    CASE WHEN element >= 86 THEN 'LARGE' ELSE 'SMALL' END AS element_tier,
    COUNT(*) AS cnt,
    SUM(CASE WHEN plan = 'basic' THEN element ELSE 0 END) AS basic_element,
    COUNT(*) AS num_rows,
    MIN(element) AS min_element,
    COUNT(CASE WHEN plan = 'pro' THEN 1 END) AS pro_count
FROM exploded
GROUP BY CASE WHEN element >= 86 THEN 'LARGE' ELSE 'SMALL' END;
