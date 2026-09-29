SELECT
    id,
    quantity,
    LEAD(unit_price) OVER w_part AS next_unit_price,
    LAG(quantity) OVER w_part AS prev_quantity,
    LEAD(quantity) OVER w_part AS next_quantity,
    ROW_NUMBER() OVER w_part AS row_seq
FROM order_items
WINDOW w_part AS (PARTITION BY order_id ORDER BY quantity, id)
