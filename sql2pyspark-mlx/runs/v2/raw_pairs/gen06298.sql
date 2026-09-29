SELECT id, rating FROM suppliers WHERE UPPER(TRIM(supplier_name)) = 'SIAM PARTS CO' AND city IN ('Bangkok', 'Singapore') ORDER BY id DESC;
