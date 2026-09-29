SELECT id, unit_price, DENSE_RANK() OVER (ORDER BY unit_price) AS unit_price_dense_rank
FROM order_items;
