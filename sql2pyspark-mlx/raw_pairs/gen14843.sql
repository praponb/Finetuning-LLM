SELECT id, product_id, unit_price, RANK() OVER (PARTITION BY product_id ORDER BY unit_price DESC) AS rnk FROM order_items;
