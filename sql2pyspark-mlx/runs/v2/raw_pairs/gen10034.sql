SELECT *, FLOOR(units) AS units_whole FROM store_sales WHERE (revenue < 2800 AND revenue > 2600) OR store_id IN (3, 4, 6);
