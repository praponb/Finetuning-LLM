SELECT
    CASE WHEN quantity >= 7 THEN 'HIGH' ELSE 'LOW' END AS quantity_tier,
    BIT_AND(quantity) AS quantity_bit_and,
    ROUND(AVG(quantity), 2) AS avg_quantity
FROM order_items
GROUP BY 1;
