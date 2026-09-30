SELECT
    id,
    store_id,
    product_id,
    quantity,
    ROW_NUMBER() OVER (PARTITION BY YEAR(last_restock) ORDER BY last_restock DESC, id) AS event_seq,
    RANK() OVER (ORDER BY quantity) AS quantity_rank,
    ROW_NUMBER() OVER w_part AS row_seq,
    LAST_VALUE(quantity, TRUE) OVER (PARTITION BY store_id, product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity,
    AVG(quantity) OVER w_recent AS quantity_avg_3
FROM inventory
WINDOW w_part AS (PARTITION BY store_id ORDER BY quantity, id), w_recent AS (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);
