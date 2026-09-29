WITH valid AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        COALESCE(city, 'N/A') AS city_filled
    FROM suppliers
    WHERE city IS DISTINCT FROM 'New York'
),
filtered AS (
    SELECT
        *,
        INITCAP(TRIM(city)) AS city_title,
        COALESCE(city, 'NONE') AS city_clean
    FROM valid
    WHERE city = 'Singapore'
),
with_latest AS (
    SELECT
        s.id,
        s.country,
        s.city,
        s.city_clean,
        latest.product_category,
        latest.product_stock_qty
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.category AS product_category, c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
matched_keys AS (
    SELECT id, country
    FROM with_latest
    EXCEPT
    SELECT id, country
    FROM suppliers
    WHERE country = 'SG'
),
reconciled AS (
    SELECT s.*
    FROM with_latest s
    JOIN matched_keys k ON s.id = k.id AND s.country = k.country
),
base AS (
    SELECT *, CONCAT(country, ' - ', product_category) AS country_product_category
    FROM reconciled
    WHERE city IS NOT NULL
      AND product_category IN ('Electronics', 'Books', 'Grocery')
)
SELECT
    country,
    city,
    city_clean,
    product_stock_qty,
    country_product_category
FROM base
ORDER BY city DESC NULLS LAST, country DESC;
