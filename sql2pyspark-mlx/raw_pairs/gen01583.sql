SELECT store_id, MIN(quantity) AS min_quantity, PERCENTILE_APPROX(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles FROM inventory GROUP BY store_id HAVING MIN(quantity) > 0;
