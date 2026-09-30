select id, launch_date, brand, supplier_id from products where (year(launch_date) = 2025 and launch_date <= '2025-09-01') or stock_qty < 291 order by supplier_id
