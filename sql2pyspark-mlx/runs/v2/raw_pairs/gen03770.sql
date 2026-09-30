select units from store_sales where (product_id in (3, 4, 6) and sale_date < '2024-10-01') or product_id = 9 order by units asc;
