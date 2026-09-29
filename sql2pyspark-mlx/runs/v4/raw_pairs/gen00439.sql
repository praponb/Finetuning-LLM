WITH base AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        DATE_FORMAT(last_restock, 'yyyy-MM-dd') AS last_restock_label
    FROM inventory
    WHERE last_restock >= TO_DATE('2024-03-24')
),
enriched AS (
    SELECT
        src.*,
        p.stock_qty,
        p.category
    FROM base src
    LEFT JOIN products p
        ON src.product_id = p.id
),
latest AS (
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity, id) = 1
)
SELECT *
FROM (
    SELECT
        store_id,
        category,
        stock_qty
    FROM latest
) src
PIVOT (
    SUM(stock_qty)
    FOR category IN ('Books', 'Sports', 'Home')
)
