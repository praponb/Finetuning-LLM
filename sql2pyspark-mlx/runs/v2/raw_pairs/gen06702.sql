WITH TopProducts AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY cost DESC, id) AS rnk FROM products) SELECT id, supplier_id, cost, product_name FROM TopProducts WHERE rnk <= 1;
