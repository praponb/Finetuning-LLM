SELECT
    *,
    LAG(quantity, 1) OVER w_ordered AS prev_quantity,
    AVG(quantity) OVER (ORDER BY last_restock, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS quantity_avg_2_prior
FROM inventory
WINDOW w_ordered AS (PARTITION BY product_id ORDER BY quantity, id);
