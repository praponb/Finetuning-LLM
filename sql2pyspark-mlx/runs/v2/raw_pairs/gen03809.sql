SELECT
    store_id,
    product_id,
    LAST_DAY(last_restock) AS last_restock_month_end,
    COALESCE(quantity, 0) AS quantity_filled
FROM inventory
ORDER BY store_id DESC, product_id;
