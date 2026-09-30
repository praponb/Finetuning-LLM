WITH cleaned AS (
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        DATE_SUB(last_restock, 79) AS last_restock_minus_79d,
        DAYOFWEEK(last_restock) AS last_restock_dayofweek
    FROM inventory
    WHERE last_restock >= TO_DATE('2023-06-29')
)
SELECT
    id,
    product_id,
    quantity,
    last_restock,
    last_restock_minus_79d,
    last_restock_dayofweek,
    ROW_NUMBER() OVER w_hist AS seq_no,
    MAX(last_restock_dayofweek) OVER (PARTITION BY product_id) AS product_id_max_last_restock_dayofweek,
    NTILE(4) OVER (PARTITION BY product_id ORDER BY quantity, id) AS quantity_bucket,
    LAG(quantity, 1) OVER (PARTITION BY product_id ORDER BY last_restock, id) AS prev_quantity,
    FIRST_VALUE(quantity) IGNORE NULLS OVER w_recent AS first_known_quantity
FROM cleaned
WINDOW w_hist AS (PARTITION BY product_id ORDER BY last_restock_minus_79d DESC, id), w_recent AS (PARTITION BY product_id ORDER BY last_restock_dayofweek, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
