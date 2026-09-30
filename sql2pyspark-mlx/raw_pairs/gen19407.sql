SELECT
    last_restock,
    quantity,
    store_id,
    DATE_TRUNC('year', last_restock) AS last_restock_period,
    LAST_DAY(last_restock) AS last_restock_month_end
FROM inventory
ORDER BY last_restock DESC;
