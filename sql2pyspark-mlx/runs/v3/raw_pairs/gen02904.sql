SELECT id, supplier_name, COALESCE(supplier_name, 'unassigned') AS supplier_name_filled FROM suppliers ORDER BY supplier_name_filled;
