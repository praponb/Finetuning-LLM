WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        brand,
        cost,
        stock_qty,
        launch_date,
        supplier_id,
        CASE WHEN cost >= 590 THEN 'High' WHEN cost >= 260 THEN 'Medium' ELSE 'Low' END AS cost_band,
        LEAST(stock_qty, cost) AS least_stock_qty_cost,
        COALESCE(brand, 'UNKNOWN') AS brand_filled
    FROM products
    WHERE launch_date >= TO_DATE('2023-03-02')
      AND brand IS DISTINCT FROM 'Stark'
)
-- Join a dimension and aggregate
SELECT
    d.country,
    d.city,
    s.brand,
    COUNT(*) AS rows_total,
    AVG(s.cost) AS avg_cost,
    GROUPING_ID() AS grouping_level
FROM filtered s
INNER JOIN suppliers d ON s.supplier_id = d.id
GROUP BY ROLLUP (d.country, d.city, s.brand)
ORDER BY rows_total NULLS FIRST;
