WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        CAST(quantity AS DOUBLE) AS quantity_num
    FROM inventory
    WHERE last_restock BETWEEN '2023-06-01' AND '2024-06-01'
),
joined AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.store_id,
        a.product_id,
        a.quantity,
        a.last_restock,
        a.quantity_num,
        b.stock_qty,
        b.price,
        b.brand
    FROM filtered a
    LEFT JOIN products b
        ON a.product_id = b.id
),
enriched AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        s.quantity,
        s.quantity_num,
        s.stock_qty,
        s.price,
        s.brand,
        p2.sqft
    FROM joined s
    LEFT JOIN stores p2
        ON s.store_id = p2.id
    WHERE MONTH(p2.opened_date) IN (1, 6, 12)
),
enriched_inventory AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY stock_qty, id) AS row_seq,
        SUM(sqft) OVER (ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft,
        AVG(price) OVER (ORDER BY stock_qty, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS price_avg_2
    FROM enriched
),
enriched_inventory_2 AS (
    -- Attach stores attributes
    SELECT
        a.*,
        p.store_name,
        p.opened_date
    FROM enriched_inventory a
    LEFT JOIN stores p
        ON a.store_id = p.id
)
SELECT
    id,
    store_id,
    quantity,
    quantity_num,
    stock_qty,
    brand,
    row_seq,
    running_sqft,
    store_name
FROM enriched_inventory_2
