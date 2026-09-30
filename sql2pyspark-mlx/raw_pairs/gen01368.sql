SELECT
    id,
    quantity,
    unit_price,
    AVG(unit_price) OVER (ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS unit_price_avg_2_prior,
    LAST_VALUE(unit_price, TRUE) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price,
    CORR(quantity, unit_price) OVER (PARTITION BY product_id) AS corr_quantity_unit_price,
    SUM(unit_price) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_unit_price
FROM order_items;
