WITH joined AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.store_id,
        e.product_id,
        e.quantity,
        e.last_restock,
        b.price
    FROM inventory e
    LEFT JOIN products b
        ON e.product_id = b.id
    WHERE b.category = 'Books'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        price,
        CAST(price AS INT) AS price_int
    FROM joined
    WHERE last_restock >= DATE '2023-09-12'
      AND MONTH(last_restock) IN (7, 9, 11)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        price_int,
        NTILE(3) OVER (PARTITION BY store_id ORDER BY quantity ASC, id ASC) AS quantity_bucket,
        FIRST_VALUE(quantity, TRUE) OVER (PARTITION BY product_id, store_id ORDER BY price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity,
        LAST_VALUE(price) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_price,
        FIRST_VALUE(quantity, TRUE) OVER (ORDER BY last_restock ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity_2
    FROM base
),
enriched AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        e.price_int,
        e.first_known_quantity,
        e.last_known_price,
        p.stock_qty
    FROM scored e
    LEFT JOIN products p
        ON e.product_id = p.id
)
SELECT
    s.id,
    s.product_id,
    s.first_known_quantity,
    s.last_known_price,
    s.stock_qty,
    (SELECT SUM(x.revenue) FROM store_sales x WHERE x.product_id = s.product_id) AS sum_store_sale_revenue
FROM enriched s
ORDER BY sum_store_sale_revenue DESC, product_id ASC
