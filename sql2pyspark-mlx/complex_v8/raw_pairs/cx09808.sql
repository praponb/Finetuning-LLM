WITH combined AS (
    -- Stack the branches
    SELECT
        qty,
        operator AS name,
        unit_price AS amount,
        'move' AS source
    FROM stock_moves
    UNION ALL
    SELECT
        id,
        sku_code,
        weight_kg,
        'sku' AS source
    FROM skus
    WHERE category IN ('Apparel', 'Beauty', 'Electronics')
    UNION ALL
    SELECT
        id,
        sku_code,
        weight_kg,
        'sku' AS source
    FROM skus
    WHERE LENGTH(TRIM(sku_code)) >= 4
      AND category IN ('Food', 'Apparel', 'Toys')
)
SELECT source, COUNT(*) AS n_rows, SUM(amount) AS total_amount
FROM combined
GROUP BY source
ORDER BY source
