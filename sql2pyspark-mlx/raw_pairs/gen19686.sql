WITH latest_match AS (
    SELECT
        s.*,
        latest.store_sale_sale_date,
        latest.revenue
    FROM products s
    JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date, c.revenue
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
base AS (
    SELECT *, ROUND(price, 1) AS price_rounded
    FROM latest_match
    WHERE brand IS NOT NULL
),
joined AS (
    SELECT
        a.id,
        a.product_name,
        a.brand,
        a.price,
        a.stock_qty,
        a.launch_date,
        a.supplier_id,
        a.store_sale_sale_date,
        b.country,
        b.city,
        b.rating
    FROM base a
    INNER JOIN suppliers b
        ON a.supplier_id = b.id
    WHERE UPPER(b.supplier_name) LIKE '%OKY%'
),
with_dims AS (
    SELECT src.*, d.country AS supplier_country
    FROM joined src
    LEFT JOIN suppliers d
        ON src.supplier_id = d.id
),
with_windows AS (
    SELECT
        *,
        LEAD(stock_qty, 1) OVER (ORDER BY store_sale_sale_date, id) AS next_stock_qty,
        LAST_VALUE(supplier_country) IGNORE NULLS OVER (PARTITION BY brand ORDER BY store_sale_sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_supplier_country,
        NTILE(3) OVER (PARTITION BY MONTH(launch_date), city ORDER BY price DESC, id) AS price_bucket
    FROM with_dims
),
filtered AS (
    SELECT
        *,
        price_bucket + stock_qty AS price_bucket_plus_stock_qty,
        COALESCE(country, 'N/A') AS country_filled
    FROM with_windows
    WHERE brand IS NOT NULL
      AND rating < 2.6
),
kept AS (
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.product_id = s.id
              AND ch.store_id IN (2, 3, 4)
        )
)
SELECT *
FROM (
    SELECT
        city,
        country,
        price
    FROM kept
) src
PIVOT (
    MAX(price)
    FOR country IN ('DE', 'US')
);
