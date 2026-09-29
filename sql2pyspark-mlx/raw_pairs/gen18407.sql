WITH filtered AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), last_restock) / 365 AS years_since_last_restock,
        DAYOFWEEK(last_restock) AS last_restock_dayofweek
    FROM inventory
    WHERE store_id = 1
),
with_dims AS (
    SELECT
        src.*,
        d.price,
        d.product_name
    FROM filtered src
    LEFT JOIN products d
        ON src.product_id = d.id
),
enriched AS (
    SELECT
        src.*,
        s.opened_date,
        s.store_name
    FROM with_dims src
    LEFT JOIN stores s
        ON src.store_id = s.id
    WHERE s.region = 'East'
),
matched_keys AS (
    SELECT store_id, product_id
    FROM enriched
    EXCEPT
    SELECT store_id, product_id
    FROM inventory
    WHERE store_id IN (1, 2, 5)
),
reconciled AS (
    SELECT s.*
    FROM enriched s
    JOIN matched_keys k ON s.store_id = k.store_id AND s.product_id = k.product_id
),
base AS (
    SELECT *, FLOOR(quantity) AS quantity_whole
    FROM reconciled
    WHERE last_restock > TO_DATE('2023-06-18')
),
matched_keys_2 AS (
    SELECT store_id, product_id
    FROM base
    INTERSECT
    SELECT store_id, product_id
    FROM inventory
    WHERE quantity BETWEEN 119 AND 159
),
kept_rows AS (
    SELECT s.*
    FROM base s
    JOIN matched_keys_2 k ON s.store_id = k.store_id AND s.product_id = k.product_id
),
enriched_inventory AS (
    SELECT
        src.id,
        src.store_id,
        src.product_id,
        src.last_restock,
        src.product_name,
        src.store_name,
        src.quantity_whole,
        b.category,
        b.price AS product_price,
        b.product_name AS product_product_name
    FROM kept_rows src
    JOIN products b
        ON src.product_id = b.id
),
summary AS (
    SELECT
        CASE WHEN quantity_whole >= 63 THEN 'HIGH' ELSE 'LOW' END AS quantity_whole_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE category = 'Electronics') AS electronics_rows,
        MAX(quantity_whole) AS max_quantity_whole,
        SORT_ARRAY(COLLECT_SET(category)) AS category_list,
        SIZE(COLLECT_SET(category)) AS num_categories,
        SORT_ARRAY(COLLECT_SET(category)) AS category_list_2
    FROM enriched_inventory
    GROUP BY 1
)
SELECT
    quantity_whole_tier,
    distinct_ids,
    electronics_rows,
    category_list,
    num_categories
FROM summary
ORDER BY distinct_ids;
