SELECT
    product_id,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    BIT_AND(quantity) AS quantity_bit_and
FROM inventory
GROUP BY product_id
ORDER BY quantity_bit_and DESC NULLS FIRST
