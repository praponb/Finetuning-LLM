SELECT id, order_id, unit_price, quantity FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS rnk FROM order_items) AS t WHERE rnk <= 2;
