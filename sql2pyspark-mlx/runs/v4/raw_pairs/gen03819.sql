WITH with_latest AS (
    SELECT
        s.*,
        x.sale_date,
        x.store_sale_units
    FROM products s
    LEFT JOIN LATERAL (
            SELECT c.sale_date, c.units AS store_sale_units
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest_match AS (
    SELECT s.*, x.quantity
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock <= s.sale_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
joined AS (
    SELECT
        s.*,
        d.rating,
        d.country,
        d.supplier_name
    FROM latest_match s
    INNER JOIN suppliers d
        ON s.supplier_id = d.id
)
SELECT
    CASE WHEN quantity >= 235 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_tier,
    CASE WHEN store_sale_units >= 43 THEN 'HIGH' ELSE 'LOW' END AS store_sale_units_tier,
    STDDEV_SAMP(price) AS price_stddev,
    MAX(cost) FILTER (WHERE country = 'UK') AS uk_max_cost,
    COUNT(*) FILTER (WHERE country = 'US') AS us_rows
FROM joined
GROUP BY 1, 2
HAVING us_rows > 1
ORDER BY us_rows DESC NULLS LAST;
