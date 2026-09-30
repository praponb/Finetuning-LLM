WITH cleaned AS (
    SELECT
        *,
        ROUND(quantity, 1) AS quantity_rounded,
        QUARTER(last_restock) AS last_restock_quarter
    FROM inventory
    WHERE last_restock >= DATE '2023-06-28'
      AND last_restock BETWEEN '2024-05-04' AND '2025-02-20'
)
SELECT
    id,
    store_id,
    product_id,
    quantity,
    quantity_rounded,
    last_restock_quarter,
    AVG(quantity_rounded) OVER w_recent AS running_avg_quantity_rounded,
    SUM(quantity_rounded) OVER w_recent AS running_sum_quantity_rounded,
    COUNT(quantity_rounded) OVER w_recent AS running_count_quantity_rounded,
    ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY last_restock_quarter, id) AS seq_no
FROM cleaned
WINDOW w_recent AS (PARTITION BY store_id ORDER BY quantity_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
