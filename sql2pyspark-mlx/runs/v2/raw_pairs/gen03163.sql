SELECT *
FROM (
    SELECT DATE_FORMAT(last_restock, 'yyyy-MM') last_restock_month, product_id, MIN(quantity) min_quantity
    FROM inventory
    GROUP BY DATE_FORMAT(last_restock, 'yyyy-MM'), product_id
) sub
WHERE min_quantity >= 150
ORDER BY product_id DESC, last_restock_month
