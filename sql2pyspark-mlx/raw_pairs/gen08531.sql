WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_FORMAT(last_restock, 'yyyy-MM-dd') AS last_restock_label
    FROM inventory
    WHERE last_restock > TO_DATE('2023-08-17')
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC NULLS FIRST, id) AS rank_in_group
        FROM cleaned
    ) x
    WHERE rank_in_group = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY rank_in_group DESC, id) AS rn
        FROM deduped
    ) t
    WHERE rn = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY store_id ORDER BY quantity DESC) AS quantity_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY rank_in_group, id) AS seq_no
    FROM latest
    WHERE store_id IN (1, 2, 6)
)
-- Join a dimension and aggregate
SELECT
    d.city,
    d.region,
    COUNT(*) AS num_rows,
    MIN(s.quantity) AS min_quantity,
    MIN(s.quantity_cume_dist) AS min_quantity_cume_dist,
    GROUPING_ID() AS level_id
FROM with_windows s
INNER JOIN stores d ON s.store_id = d.id
GROUP BY CUBE (d.city, d.region)
ORDER BY city DESC NULLS LAST;
