SELECT DATE_FORMAT(last_restock, 'yyyy-MM') last_restock_month, ROUND(AVG(quantity), 2) avg_quantity, COUNT(quantity) quantity_count FROM inventory GROUP BY last_restock_month
