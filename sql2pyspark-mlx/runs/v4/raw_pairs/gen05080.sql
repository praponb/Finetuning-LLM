SELECT order_id, PERCENTILE_APPROX(quantity, 0.9) AS quantity_p90, STDDEV_SAMP(quantity) AS quantity_stddev FROM order_items GROUP BY order_id ORDER BY quantity_stddev;
