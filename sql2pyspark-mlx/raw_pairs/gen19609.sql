WITH valid AS (
    SELECT
        *,
        COALESCE(cost, 0) cost_filled,
        COALESCE(brand, 'N/A') brand_filled
    FROM products
    WHERE brand IS NOT DISTINCT FROM 'Umbrella'
      AND supplier_id IS NOT NULL
),
enriched AS (
    SELECT
        id,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        cost_filled,
        AVG(cost) OVER (PARTITION BY DATE_TRUNC('WEEK', launch_date), supplier_id ORDER BY cost_filled ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) cost_avg_2_prior,
        AVG(stock_qty) OVER (ORDER BY price ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) stock_qty_avg_2,
        ROW_NUMBER() OVER (ORDER BY cost ASC, id ASC) row_seq
    FROM valid
),
eligible AS (
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.units != 36
        )
),
kept AS (
    SELECT s.*
    FROM eligible s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.product_id = s.id
              AND ch.sale_date < s.launch_date
        )
)
SELECT
    d.city,
    e.brand,
    COUNT(*) AS rows_total,
    ROUND(AVG(e.price), 2) AS avg_price,
    AVG(e.cost_filled) AS avg_cost_filled
FROM kept e
LEFT JOIN suppliers d ON e.supplier_id = d.id
GROUP BY d.city, e.brand;
