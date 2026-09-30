WITH valid AS (
    SELECT *, ROUND(quantity, 1) AS quantity_rounded
    FROM inventory
    WHERE last_restock > '2024-03-03'
),
with_dims AS (
    SELECT
        src.*,
        d.brand,
        d.category
    FROM valid src
    INNER JOIN products d
        ON src.product_id = d.id
),
with_windows AS (
    SELECT
        *,
        SUM(quantity) OVER (PARTITION BY store_id, category ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        ROW_NUMBER() OVER (ORDER BY quantity_rounded DESC, id) AS row_seq
    FROM with_dims
),
metrics AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        quantity_rounded,
        brand,
        category,
        cumulative_quantity,
        row_seq,
        NTILE(3) OVER (PARTITION BY product_id ORDER BY quantity_rounded DESC, id) AS quantity_rounded_bucket,
        STDDEV_SAMP(quantity) OVER (PARTITION BY store_id) AS store_id_stddev_samp_quantity,
        STDDEV_SAMP(quantity) OVER (PARTITION BY store_id) AS store_id_stddev_samp_quantity_2,
        LAST_VALUE(cumulative_quantity) IGNORE NULLS OVER (ORDER BY quantity_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cumulative_quantity
    FROM with_windows
),
valid_inventory AS (
    SELECT
        id,
        store_id,
        last_restock,
        quantity_rounded,
        brand,
        category,
        cumulative_quantity,
        row_seq,
        quantity_rounded_bucket,
        store_id_stddev_samp_quantity,
        store_id_stddev_samp_quantity_2,
        last_known_cumulative_quantity,
        GREATEST(quantity_rounded_bucket, quantity_rounded) AS greatest_quantity_rounded_bucket_quantity_rounded
    FROM metrics
    WHERE last_restock >= TO_DATE('2023-09-15')
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand, category ORDER BY greatest_quantity_rounded_bucket_quantity_rounded DESC, id) AS rn
        FROM valid_inventory
    ) t
    WHERE rn = 1
)
SELECT
    s.id,
    s.store_id,
    s.last_restock,
    s.quantity_rounded,
    s.brand,
    s.category,
    s.cumulative_quantity,
    s.row_seq,
    s.quantity_rounded_bucket,
    CASE WHEN category RLIKE '^(Grocery|Books)$' THEN 'GROUP_A' WHEN category RLIKE '^(Electronics|Home)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS category_zone,
    (SELECT AVG(x.quantity) FROM inventory x WHERE x.store_id = s.store_id) AS avg_inventory_quantity
FROM latest s
ORDER BY id DESC;
