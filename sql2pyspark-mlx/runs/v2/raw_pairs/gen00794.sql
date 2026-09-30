SELECT *, COALESCE(units, 0) AS units_filled, ROUND(units, 2) AS units_rounded FROM store_sales WHERE units <= 26 AND (store_id = 1 OR revenue < 3800) ORDER BY id DESC, sale_date;
