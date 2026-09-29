WITH latest AS (
    SELECT *
    FROM inventory
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY quantity DESC NULLS FIRST, id ASC) = 1
),
filtered AS (
    SELECT *, ROUND(quantity, 2) AS quantity_rounded
    FROM latest
    WHERE quantity < 197
),
metrics AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        SUM(quantity_rounded) OVER w_ordered AS last4_sum_quantity_rounded,
        MAX(quantity_rounded) OVER w_ordered AS last4_max_quantity_rounded,
        AVG(quantity_rounded) OVER w_ordered AS last4_avg_quantity_rounded
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY product_id ORDER BY last_restock ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock ASC NULLS LAST, id ASC) AS row_num
    FROM metrics
    QUALIFY row_num = 1
),
scored AS (
    SELECT
        *,
        COUNT(last4_sum_quantity_rounded) OVER w_part AS last7_count_last4_sum_quantity_rounded,
        SUM(last4_sum_quantity_rounded) OVER w_part AS last7_sum_last4_sum_quantity_rounded,
        MIN(last4_sum_quantity_rounded) OVER w_part AS last7_min_last4_sum_quantity_rounded
    FROM ranked
    WHERE product_id IN (2, 4, 6)
    WINDOW w_part AS (PARTITION BY store_id, product_id ORDER BY row_num ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.quantity,
    s.last_restock,
    s.last4_sum_quantity_rounded,
    s.last4_max_quantity_rounded,
    s.last4_avg_quantity_rounded,
    s.row_num,
    (SELECT AVG(x.revenue) FROM store_sales x WHERE x.store_id = s.store_id) AS avg_store_sale_revenue
FROM scored s
ORDER BY row_num DESC
