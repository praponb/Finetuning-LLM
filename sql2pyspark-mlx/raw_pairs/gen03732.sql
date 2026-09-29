WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(quantity) IGNORE NULLS OVER (PARTITION BY store_id, product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity,
        NTILE(3) OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS quantity_bucket,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS seq_no
    FROM inventory
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY last_restock DESC, id) AS rank_in_group
    FROM metrics
    QUALIFY rank_in_group = 1
),
enriched AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.last_restock,
        s.quantity_bucket,
        s.rank_in_group,
        d.category,
        d.brand,
        d.product_name
    FROM ranked s
    INNER JOIN products d
        ON s.product_id = d.id
),
with_dims AS (
    -- Attach products attributes
    SELECT src.*, d.product_name AS product_product_name
    FROM enriched src
    INNER JOIN products d
        ON src.product_id = d.id
)
SELECT
    s.id,
    s.product_id,
    s.rank_in_group,
    s.category,
    s.brand,
    s.product_product_name,
    CASE WHEN product_product_name RLIKE '^(Noise-Cancel Headphones|wireless mouse)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS product_product_name_zone,
    (SELECT MIN(t2.quantity) FROM inventory t2 WHERE t2.product_id = s.product_id) AS min_inventory_quantity
FROM with_dims s
ORDER BY product_product_name_zone DESC NULLS LAST;
