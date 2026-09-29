WITH valid AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        CEIL(quantity) quantity_whole
    FROM inventory
    WHERE last_restock > DATE '2023-06-09'
      AND product_id IN (7, 8, 10)
),
remaining_keys AS (
    SELECT product_id, store_id
    FROM valid
    EXCEPT ALL
    SELECT product_id, store_id
    FROM inventory
    WHERE last_restock > '2024-04-07'
),
kept_rows AS (
    SELECT s.*
    FROM valid s
    JOIN remaining_keys k ON s.product_id = k.product_id AND s.store_id = k.store_id
),
cleaned AS (
    SELECT *, COALESCE(quantity, 0) quantity_filled
    FROM kept_rows
    WHERE quantity_whole BETWEEN 31 AND 73
),
enriched AS (
    SELECT
        a.*,
        s.region,
        s.store_name,
        s.city
    FROM cleaned a
    LEFT JOIN stores s
        ON a.store_id = s.id
),
filtered AS (
    SELECT
        id,
        store_id,
        quantity,
        last_restock,
        quantity_filled,
        region,
        store_name,
        city,
        ROUND(quantity_filled, 2) quantity_filled_rounded,
        UPPER(TRIM(store_name)) store_name_normalized,
        COALESCE(region, 'UNKNOWN') resolved_region
    FROM enriched
    WHERE region IS NOT NULL
)
SELECT
    store_id,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list_2,
    COUNT(CASE WHEN city = 'New York' THEN 1 END) AS new_york_count
FROM filtered
GROUP BY CUBE (store_id)
