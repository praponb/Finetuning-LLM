WITH cleaned AS (
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        supplier_id,
        INITCAP(TRIM(product_name)) AS product_name_title
    FROM products
    WHERE supplier_id IS NOT NULL
      AND launch_date > DATE '2023-08-24'
),
with_dims AS (
    SELECT
        e.id,
        e.category,
        e.price,
        e.cost,
        e.stock_qty,
        e.supplier_id,
        e.product_name_title,
        b.city,
        b.rating
    FROM cleaned e
    LEFT JOIN suppliers b
        ON e.supplier_id <=> b.id
    WHERE b.city IN ('Bangkok', 'London')
),
enriched AS (
    SELECT
        id,
        supplier_id,
        city,
        rating,
        MIN(stock_qty) OVER (PARTITION BY city, supplier_id ORDER BY cost, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_stock_qty,
        COUNT(stock_qty) OVER (PARTITION BY city, supplier_id ORDER BY cost, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_stock_qty,
        MAX(stock_qty) OVER (PARTITION BY city, supplier_id ORDER BY cost, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_stock_qty
    FROM with_dims
),
latest AS (
    SELECT
        id,
        supplier_id,
        city,
        rating,
        last4_max_stock_qty
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY last4_min_stock_qty DESC, id) AS latest_rank
        FROM enriched
    ) x
    WHERE latest_rank = 1
),
enriched_products AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY supplier_id ORDER BY last4_max_stock_qty DESC NULLS FIRST) AS last4_max_stock_qty_cume_dist,
        CUME_DIST() OVER w_recent AS rating_cume_dist
    FROM latest
    WINDOW w_recent AS (ORDER BY rating DESC NULLS FIRST)
)
SELECT
    s.id,
    s.supplier_id,
    s.city,
    s.rating,
    s.last4_max_stock_qty_cume_dist,
    (SELECT MIN(x.price) FROM products x WHERE x.supplier_id = s.supplier_id) AS min_product_price
FROM enriched_products s
ORDER BY min_product_price DESC NULLS FIRST;
