WITH cleaned AS (
    SELECT
        id,
        store_id,
        quantity,
        FLOOR(quantity) AS quantity_whole
    FROM inventory
    WHERE product_id IN (1, 3, 5, 8)
),
kept_keys AS (
    SELECT store_id, id
    FROM cleaned
    INTERSECT ALL
    SELECT store_id, id
    FROM inventory
    WHERE store_id IN (3, 5)
),
verified AS (
    SELECT s.*
    FROM cleaned s
    JOIN kept_keys k ON s.store_id = k.store_id AND s.id = k.id
),
scored AS (
    SELECT
        *,
        AVG(quantity_whole) OVER w_part AS store_id_avg_quantity_whole,
        STDDEV_SAMP(quantity_whole) OVER w_part AS store_id_stddev_samp_quantity_whole,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY quantity_whole DESC NULLS FIRST) AS quantity_whole_percent_rank,
        SUM(quantity) OVER w_part AS store_id_sum_quantity,
        PERCENT_RANK() OVER w_hist AS quantity_percent_rank
    FROM verified
    WINDOW w_part AS (PARTITION BY store_id), w_hist AS (PARTITION BY store_id ORDER BY quantity)
),
cleaned_inventory AS (
    SELECT *, quantity_percent_rank + quantity AS quantity_percent_rank_plus_quantity
    FROM scored
    WHERE quantity BETWEEN 145 AND 199
),
enriched AS (
    SELECT e.*, d.store_name
    FROM cleaned_inventory e
    JOIN stores d
        ON e.store_id = d.id
),
enriched_inventory AS (
    SELECT src.*, s.city
    FROM enriched src
    JOIN stores s
        ON src.store_id = s.id
)
SELECT
    id,
    store_id,
    quantity,
    quantity_whole,
    store_id_avg_quantity_whole,
    store_id_stddev_samp_quantity_whole,
    quantity_whole_percent_rank,
    store_id_sum_quantity,
    quantity_percent_rank
FROM enriched_inventory
ORDER BY quantity_whole_percent_rank
