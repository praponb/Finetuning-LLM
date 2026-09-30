SELECT i.id, i.store_id, i.last_restock, p.product_name, p.supplier_id, DATEDIFF(CURRENT_DATE(), i.last_restock) AS days_since_last_restock FROM inventory i JOIN products p ON i.product_id = p.id;
