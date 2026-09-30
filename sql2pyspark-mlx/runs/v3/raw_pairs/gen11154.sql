select store_id, date_format(last_restock, 'MMM yyyy') as last_restock_label from inventory where store_id in (1, 2, 5)
