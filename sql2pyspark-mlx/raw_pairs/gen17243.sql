WITH valid AS (
    SELECT *, ROUND(stock_qty, 0) AS stock_qty_rounded
    FROM products
    WHERE supplier_id IS NOT NULL
      AND cost BETWEEN 420 AND 700
),
filtered AS (
    SELECT
        id,
        product_name,
        category,
        launch_date,
        tags,
        supplier_id,
        stock_qty_rounded,
        CASE WHEN category = 'Fashion' THEN 1 ELSE 0 END AS is_fashion,
        UPPER(TRIM(product_name)) AS product_name_normalized,
        COALESCE(brand, 'UNKNOWN') AS resolved_brand
    FROM valid
    WHERE launch_date >= DATE '2023-05-08'
      AND supplier_id IS NOT NULL
),
metrics AS (
    SELECT
        *,
        ROW_NUMBER() OVER w_hist AS row_seq,
        CUME_DIST() OVER (PARTITION BY DATE_TRUNC('WEEK', launch_date) ORDER BY is_fashion DESC NULLS FIRST) AS is_fashion_cume_dist
    FROM filtered
    WINDOW w_hist AS (ORDER BY stock_qty_rounded, id)
),
enriched AS (
    SELECT e.*, p.city
    FROM metrics e
    LEFT JOIN suppliers p
        ON e.supplier_id = p.id
),
enriched_products AS (
    SELECT
        e.*,
        d.country,
        d.rating
    FROM enriched e
    LEFT JOIN suppliers d
        ON e.supplier_id = d.id
),
with_latest AS (
    SELECT s.*, x.quantity
    FROM enriched_products s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY category, country ORDER BY launch_date NULLS LAST) AS row_num
    FROM with_latest
    QUALIFY row_num = 1
)
SELECT
    p.country AS supplier_country,
    s.category,
    COUNT(*) AS cnt,
    MAX(s.row_seq) AS max_row_seq,
    GROUPING_ID() AS gid
FROM ranked s
JOIN suppliers p ON s.supplier_id = p.id
GROUP BY ROLLUP (p.country, s.category)
