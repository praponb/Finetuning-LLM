WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, DAY(last_restock) AS last_restock_day
    FROM inventory
    WHERE last_restock > DATE '2023-09-02'
      AND product_id IN (2, 9, 10)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock_day
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock NULLS LAST, id) = 1
),
enriched AS (
    -- Attach products attributes
    SELECT
        e.*,
        b.category,
        b.stock_qty
    FROM latest e
    JOIN products b
        ON e.product_id = b.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(stock_qty) OVER (ORDER BY stock_qty, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS stock_qty_avg_3_prior,
        LAST_VALUE(quantity, TRUE) OVER (PARTITION BY category, store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity,
        LAG(stock_qty, 1) OVER (ORDER BY stock_qty, id) AS prev_stock_qty,
        AVG(last_restock_day) OVER (PARTITION BY store_id) AS store_id_avg_last_restock_day
    FROM enriched
)
SELECT
    s.id,
    s.product_id,
    s.last_restock_day,
    s.category,
    s.last_known_quantity,
    s.prev_stock_qty,
    s.store_id_avg_last_restock_day,
    (SELECT AVG(t2.units) FROM store_sales t2 WHERE t2.product_id = s.product_id) AS avg_store_sale_units
FROM scored s;
