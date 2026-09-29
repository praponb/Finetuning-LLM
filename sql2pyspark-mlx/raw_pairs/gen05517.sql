WITH joined AS (
    SELECT
        e.*,
        s.city,
        s.supplier_name,
        s.country
    FROM products e
    LEFT JOIN suppliers s
        ON e.supplier_id = s.id
),
deduped AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        stock_qty,
        tags,
        city
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY cost DESC, id) = 1
),
kept AS (
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.revenue >= 2300
        )
),
with_windows AS (
    SELECT
        *,
        SUM(stock_qty) OVER w_hist AS cumulative_stock_qty,
        RANK() OVER (ORDER BY stock_qty DESC NULLS LAST) AS stock_qty_rank,
        AVG(stock_qty) OVER (PARTITION BY category ORDER BY stock_qty, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS stock_qty_avg_3_prior
    FROM kept
    WHERE brand <> 'Initech'
    WINDOW w_hist AS (PARTITION BY city ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    brand,
    BIT_OR(stock_qty) AS stock_qty_bit_or,
    SIZE(COLLECT_SET(brand)) AS num_brands,
    ROUND(SUM(stock_qty) / NULLIF(COUNT(CASE WHEN city = 'Tokyo' THEN 1 END), 0), 2) AS stock_qty_per_tokyo,
    PERCENTILE_APPROX(cumulative_stock_qty, 0.5) AS cumulative_stock_qty_p50,
    APPROX_PERCENTILE(stock_qty, 0.95) AS stock_qty_p95,
    COUNT(*) FILTER (WHERE city = 'London') AS london_rows
FROM with_windows
GROUP BY brand
HAVING stock_qty_per_tokyo > 0
ORDER BY stock_qty_p95 DESC;
