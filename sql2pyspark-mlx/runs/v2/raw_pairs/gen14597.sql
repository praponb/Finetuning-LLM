select store_id, round(revenue / units, 2) as revenue_per_unit from store_sales where units between 12 and 28 or product_id in (2, 3, 5, 10);
