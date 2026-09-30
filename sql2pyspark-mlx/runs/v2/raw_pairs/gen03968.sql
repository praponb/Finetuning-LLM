SELECT YEAR(last_restock) last_restock_year, COUNT(quantity) quantity_count FROM inventory GROUP BY YEAR(last_restock) HAVING AVG(quantity) >= 120;
