SELECT id, unit_price, PERCENT_RANK() OVER (ORDER BY unit_price DESC) AS unit_price_pct_rank
FROM order_items
WHERE order_id IN (16, 17);
