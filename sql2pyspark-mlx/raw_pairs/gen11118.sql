WITH base AS (
    SELECT *, COALESCE(revenue, 0) AS revenue_filled
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-02-15')
),
with_dims AS (
    SELECT
        src.id,
        src.product_id,
        src.units,
        src.revenue,
        src.revenue_filled,
        d.stock_qty
    FROM base src
    LEFT JOIN products d
        ON src.product_id = d.id
),
kept_keys AS (
    SELECT product_id, id
    FROM with_dims
    INTERSECT ALL
    SELECT product_id, id
    FROM store_sales
    WHERE MONTH(sale_date) IN (7, 8, 9)
),
kept_rows AS (
    SELECT s.*
    FROM with_dims s
    INNER JOIN kept_keys k ON s.product_id = k.product_id AND s.id = k.id
),
with_dims_store_sales AS (
    SELECT
        src.*,
        p.stock_qty AS product_stock_qty,
        p.price
    FROM kept_rows src
    LEFT JOIN products p
        ON src.product_id = p.id
),
joined AS (
    SELECT src.*, p.brand
    FROM with_dims_store_sales src
    LEFT JOIN products p
        ON src.product_id = p.id
    WHERE p.supplier_id IN (6, 11, 12, 27)
)
SELECT
    s.id,
    s.product_id,
    s.revenue,
    s.revenue_filled,
    s.stock_qty,
    s.product_stock_qty,
    s.price,
    (SELECT SUM(t2.units) FROM store_sales t2 WHERE t2.product_id = s.product_id) AS sum_store_sale_units
FROM joined s
ORDER BY price
