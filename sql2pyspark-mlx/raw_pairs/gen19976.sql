SELECT
    id,
    unit_price,
    LAG(quantity, 2) OVER (PARTITION BY product_id ORDER BY quantity, id) AS prev_quantity,
    LEAD(quantity, 1) OVER (PARTITION BY product_id ORDER BY unit_price, id) AS next_quantity,
    ROW_NUMBER() OVER w_running AS row_seq
FROM order_items
WINDOW w_running AS (ORDER BY quantity DESC, id);
