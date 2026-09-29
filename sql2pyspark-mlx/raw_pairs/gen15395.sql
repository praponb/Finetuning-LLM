WITH latest AS (
    SELECT *
    FROM products
    QUALIFY RANK() OVER (PARTITION BY category ORDER BY cost DESC NULLS FIRST) = 1
)
SELECT
    CASE WHEN stock_qty >= 444 THEN 'HIGH' ELSE 'LOW' END AS stock_qty_tier,
    COUNT(*) FILTER (WHERE brand = 'Umbrella') AS umbrella_rows,
    ROUND(AVG(stock_qty), 2) AS avg_stock_qty,
    BIT_OR(stock_qty) AS stock_qty_bit_or
FROM latest
GROUP BY CASE WHEN stock_qty >= 444 THEN 'HIGH' ELSE 'LOW' END
HAVING COUNT(*) FILTER (WHERE brand = 'Umbrella') > 1
ORDER BY avg_stock_qty DESC NULLS FIRST;
