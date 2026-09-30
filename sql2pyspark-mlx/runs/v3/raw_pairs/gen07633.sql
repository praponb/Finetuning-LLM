SELECT units FROM store_sales WHERE sale_date <= '2023-10-12' UNION ALL SELECT units FROM store_sales WHERE MONTH(sale_date) IN (2, 3, 5) ORDER BY units;
