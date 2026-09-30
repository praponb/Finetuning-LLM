SELECT quantity, FLOOR(quantity) quantity_whole, DATE_FORMAT(last_restock, 'yyyy-MM') last_restock_label
FROM inventory
