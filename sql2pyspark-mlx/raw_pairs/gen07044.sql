WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        DATE_FORMAT(last_restock, 'yyyy-MM') last_restock_label
    FROM inventory
    WHERE last_restock >= DATE '2023-08-27'
      AND MONTH(last_restock) IN (1, 4, 10)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_count_quantity,
        MAX(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_max_quantity,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_sum_quantity
    FROM base
),
enriched_inventory AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        src.last_restock,
        src.last_restock_label,
        src.last3_max_quantity,
        src.last3_sum_quantity,
        p.category,
        p.product_name
    FROM enriched src
    INNER JOIN products p
        ON src.product_id = p.id
),
joined AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.product_id,
        a.last_restock,
        a.last_restock_label,
        a.last3_max_quantity,
        a.category,
        a.product_name,
        d.stock_qty
    FROM enriched_inventory a
    LEFT JOIN products d
        ON a.product_id = d.id
    WHERE d.price <= 370
),
joined_inventory AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        src.last_restock,
        src.last_restock_label,
        src.last3_max_quantity,
        src.category,
        src.stock_qty,
        p.launch_date,
        p.brand,
        p.price
    FROM joined src
    LEFT JOIN products p
        ON src.product_id = p.id
)
SELECT
    id,
    last_restock_label,
    last3_max_quantity,
    category,
    launch_date,
    brand,
    CASE WHEN category RLIKE '^(Electronics|Fashion)$' THEN 'EXTENDED' WHEN category RLIKE '^(Home|Sports)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS category_zone
FROM joined_inventory
ORDER BY brand DESC NULLS FIRST;
