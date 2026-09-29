SELECT id, store_id, units, PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY units DESC) AS units_pct_rank FROM store_sales WHERE product_id IN (3, 7);
