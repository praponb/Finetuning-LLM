SELECT
    *,
    SUM(unit_price) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
    AVG(unit_price) OVER (PARTITION BY order_id) AS order_id_avg_unit_price
FROM order_items;
