WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, revenue + units AS revenue_per_unit
    FROM store_sales
    WHERE store_id IN (1, 5, 6)
),
joined AS (
    -- Attach products attributes
    SELECT
        a.*,
        d.brand,
        d.product_name
    FROM filtered a
    LEFT JOIN products d
        ON a.product_id = d.id
    WHERE MONTH(d.launch_date) IN (2, 5, 8)
),
enriched AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        s.sale_date,
        s.units,
        s.revenue,
        s.revenue_per_unit,
        s.brand,
        p.stock_qty,
        p.price,
        p.cost
    FROM joined s
    LEFT JOIN products p
        ON s.product_id = p.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(price * 1.15, 2) AS adjusted_price
    FROM enriched
    WHERE sale_date >= DATE '2023-04-17'
      AND revenue_per_unit >= 55
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue_per_unit,
        brand,
        stock_qty,
        price,
        cost,
        cost * price AS margin,
        COALESCE(brand, 'OTHER') AS brand_filled
    FROM base
    WHERE sale_date > TO_DATE('2023-04-21')
      AND brand IS NOT NULL
)
-- Multi-level aggregation
SELECT
    store_id,
    COUNT(*) AS event_count,
    AVG(price) AS avg_price,
    BIT_OR(units) AS units_bit_or,
    BIT_AND(units) AS units_bit_and,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
    COUNT(CASE WHEN brand = 'Acme' THEN 1 END) AS acme_count
FROM valid
GROUP BY store_id
ORDER BY store_id NULLS LAST
