SELECT
    id,
    order_id,
    product_id,
    FIRST_VALUE(quantity, TRUE) OVER (PARTITION BY order_id ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_quantity,
    RANK() OVER (PARTITION BY order_id ORDER BY quantity ASC NULLS FIRST) quantity_rank,
    SUM(unit_price) OVER (PARTITION BY order_id) order_id_sum_unit_price,
    SUM(unit_price) OVER (PARTITION BY product_id ORDER BY unit_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_unit_price
FROM order_items;
