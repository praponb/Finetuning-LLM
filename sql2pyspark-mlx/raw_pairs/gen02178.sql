SELECT
    id,
    product_id,
    quantity,
    DATE_TRUNC('YEAR', last_restock) AS last_restock_period,
    ADD_MONTHS(last_restock, 3) AS last_restock_plus_months
FROM inventory;
