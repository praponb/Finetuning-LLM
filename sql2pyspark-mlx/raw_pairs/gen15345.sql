SELECT
    id,
    store_id,
    product_id,
    quantity,
    LEAD(quantity, 2) OVER w_grp AS next_quantity,
    AVG(quantity) OVER (PARTITION BY store_id, product_id ORDER BY quantity, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS quantity_avg_5_prior
FROM inventory
WINDOW w_grp AS (PARTITION BY store_id ORDER BY quantity, id)
