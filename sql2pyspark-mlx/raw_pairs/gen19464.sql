WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY revenue DESC NULLS LAST, id) AS rn
    FROM store_sales
    QUALIFY rn = 1
),
enriched AS (
    -- Attach products attributes
    SELECT a.*, p.stock_qty
    FROM ranked a
    LEFT JOIN products p
        ON a.product_id = p.id
    WHERE p.category = 'Books'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        sale_date,
        rn,
        stock_qty,
        LEAD(units) OVER w_grp AS next_units,
        LEAD(revenue) OVER w_grp AS next_revenue,
        ROW_NUMBER() OVER w_grp AS event_seq
    FROM enriched
    WINDOW w_grp AS (PARTITION BY product_id, store_id ORDER BY sale_date, id)
),
joined AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.rn,
        s.next_units,
        s.event_seq,
        d.category,
        d.brand
    FROM metrics s
    INNER JOIN products d
        ON s.product_id = d.id
),
enriched_store_sales AS (
    -- Attach products attributes
    SELECT src.*, p.price
    FROM joined src
    INNER JOIN products p
        ON src.product_id = p.id
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            product_id,
            brand,
            price
        FROM enriched_store_sales
    ) src
    PIVOT (
        SUM(price) AS total,
        COUNT(1) AS item_count
        FOR brand IN ('Stark' AS stark, 'Umbrella' AS umbrella)
    )
)
SELECT s.product_id, (SELECT MAX(o2.unit_price) FROM order_items o2 WHERE o2.product_id = s.product_id) AS max_order_item_unit_price
FROM pivoted s
ORDER BY product_id DESC NULLS FIRST
