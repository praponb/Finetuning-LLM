SELECT
    id,
    product_id,
    ROUND(quantity, 1) quantity_rounded,
    ROUND(quantity, 0) quantity_rounded_2
FROM inventory;
