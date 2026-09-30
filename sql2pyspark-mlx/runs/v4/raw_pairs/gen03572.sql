WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        DAY(last_restock) AS last_restock_day
    FROM inventory
    WHERE last_restock >= DATE '2023-12-07'
      AND product_id IN (3, 6, 10)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        last_restock_day,
        CUME_DIST() OVER (PARTITION BY store_id ORDER BY last_restock_day NULLS FIRST) AS last_restock_day_cume_dist,
        AVG(quantity) OVER w_ordered AS quantity_avg_6,
        LEAD(quantity, 1) OVER w_ordered_2 AS next_quantity,
        COUNT(quantity) OVER (PARTITION BY store_id) AS store_id_count_quantity,
        COUNT(last_restock_day) OVER w_grp AS store_id_count_last_restock_day
    FROM valid
    WINDOW w_ordered AS (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_ordered_2 AS (PARTITION BY product_id ORDER BY last_restock_day, id), w_grp AS (PARTITION BY store_id, product_id)
),
enriched_inventory AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(store_id_count_quantity, 1) OVER (ORDER BY quantity_avg_6, id) AS next_store_id_count_quantity,
        CUME_DIST() OVER (PARTITION BY product_id, store_id ORDER BY store_id_count_quantity) AS store_id_count_quantity_cume_dist,
        PERCENT_RANK() OVER w_part AS store_id_count_last_restock_day_percent_rank
    FROM enriched
    WINDOW w_part AS (PARTITION BY store_id ORDER BY store_id_count_last_restock_day DESC NULLS FIRST)
)
-- Ranking and gap features
SELECT
    *,
    LAG(last_restock_day_cume_dist, 2) OVER (PARTITION BY store_id ORDER BY quantity_avg_6, id) AS prev_last_restock_day_cume_dist,
    ROW_NUMBER() OVER w_ordered AS seq_no,
    AVG(store_id_count_quantity) OVER w_grp AS store_id_count_quantity_avg_3,
    NTILE(3) OVER w_running AS quantity_avg_6_bucket
FROM enriched_inventory
WINDOW w_ordered AS (PARTITION BY product_id ORDER BY last_restock, id), w_grp AS (ORDER BY store_id_count_quantity_cume_dist, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_running AS (ORDER BY quantity_avg_6, id);
