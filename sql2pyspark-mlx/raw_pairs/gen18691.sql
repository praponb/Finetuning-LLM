select sale_date, revenue, round(units, 1) as units_rounded from store_sales where store_id in (3, 4);
