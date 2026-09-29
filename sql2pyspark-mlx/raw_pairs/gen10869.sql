WITH base AS (
    SELECT
        id,
        category,
        brand,
        price,
        stock_qty,
        tags,
        supplier_id,
        COALESCE(product_name, 'N/A') AS product_name_filled,
        CASE WHEN brand = 'Acme' THEN 1 ELSE 0 END AS is_acme
    FROM products
    WHERE supplier_id IS NOT NULL
      AND brand IS DISTINCT FROM 'Initech'
),
deduped AS (
    SELECT id, product_name_filled
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY stock_qty DESC NULLS LAST, id) AS rn
        FROM base
    ) x
    WHERE rn = 1
),
with_latest AS (
    SELECT
        s.*,
        top1.sale_date,
        top1.revenue
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.sale_date, c.revenue
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
filtered AS (
    SELECT *, COALESCE(revenue, 0) AS revenue_filled
    FROM with_latest
    WHERE sale_date < '2023-10-09'
),
with_latest_products AS (
    SELECT
        s.*,
        x.units,
        x.store_sale_sale_date
    FROM filtered s
    JOIN LATERAL (
            SELECT c.units, c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT *
FROM (
    SELECT
        id,
        revenue,
        revenue_filled
    FROM with_latest_products
) src
UNPIVOT (
    val FOR kpi IN (revenue, revenue_filled AS REVENUE_FILLED)
);
