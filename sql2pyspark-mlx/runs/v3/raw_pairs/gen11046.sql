SELECT YEAR(last_restock) AS last_restock_year, SUM(quantity) AS total_quantity, MAX(last_restock) AS latest_last_restock FROM inventory GROUP BY YEAR(last_restock)
