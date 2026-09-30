SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity, id) AS rnk FROM order_items WHERE unit_price BETWEEN 210 AND 400) AS r WHERE rnk <= 3
