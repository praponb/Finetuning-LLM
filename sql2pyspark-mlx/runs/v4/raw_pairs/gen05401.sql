SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price, id) AS rnk FROM order_items WHERE quantity < 3) AS ranked WHERE rnk <= 1;
