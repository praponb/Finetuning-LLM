WITH src AS (SELECT * FROM order_items WHERE product_id = 30), Agg AS (SELECT order_id, SUM(unit_price) AS total_unit_price FROM src GROUP BY order_id) SELECT * FROM Agg
