SELECT store_name, 'store' AS kind FROM stores WHERE sqft <> 4866 UNION ALL SELECT level, 'employee' AS kind FROM employees ORDER BY kind;
