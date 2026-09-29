SELECT
    CASE WHEN quantity >= 8 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    CORR(unit_price, quantity) AS corr_unit_price_quantity,
    SUM(quantity) AS total_quantity,
    COUNT(*) AS cnt
FROM order_items
GROUP BY CASE WHEN quantity >= 8 THEN 'LARGE' ELSE 'SMALL' END
HAVING CORR(unit_price, quantity) >= 0;
