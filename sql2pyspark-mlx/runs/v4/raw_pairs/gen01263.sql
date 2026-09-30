select id, product_id, quantity, last_restock, date_format(last_restock, 'MMM yyyy') last_restock_label from inventory where store_id in (2, 3, 4, 5) and quantity <= 197;
