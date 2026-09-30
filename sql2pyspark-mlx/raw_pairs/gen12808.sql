SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY last_restock DESC NULLS LAST, id) AS row_num FROM inventory QUALIFY row_num = 1;
