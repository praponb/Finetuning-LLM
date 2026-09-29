SELECT
    id,
    unit_price,
    order_id,
    quantity,
    ROW_NUMBER() OVER (ORDER BY unit_price DESC, id) AS rn,
    PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY quantity DESC) AS quantity_pct_rank
FROM order_items
