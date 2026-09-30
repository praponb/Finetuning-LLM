select id, sale_date, store_id, product_id, units * revenue as revenue_per_unit from store_sales where units <= 28 or product_id in (7, 10);
