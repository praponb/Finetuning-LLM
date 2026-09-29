SELECT last_restock FROM inventory WHERE quantity >= 156 EXCEPT SELECT sale_date AS last_restock FROM store_sales WHERE sale_date <= '2025-08-01';
