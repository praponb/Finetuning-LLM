WITH valid AS (
    SELECT
        *,
        DATE_FORMAT(last_restock, 'yyyy-MM-dd') AS last_restock_label,
        CEIL(quantity) AS quantity_whole
    FROM inventory
    WHERE last_restock > TO_DATE('2023-10-23')
),
with_windows AS (
    SELECT
        *,
        MAX(quantity) OVER (PARTITION BY store_id) AS store_id_max_quantity,
        ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity_whole, id) AS event_seq,
        ROW_NUMBER() OVER (ORDER BY last_restock DESC, id) AS event_seq_2,
        LEAD(quantity, 2) OVER (PARTITION BY store_id, product_id ORDER BY quantity_whole, id) AS next_quantity,
        ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity_whole DESC, id) AS seq_no
    FROM valid
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity, id) AS rn
        FROM with_windows
    ) x
    WHERE rn = 1
),
with_windows_inventory AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY product_id ORDER BY seq_no DESC NULLS FIRST) AS seq_no_rank,
        LAST_VALUE(store_id_max_quantity, TRUE) OVER (PARTITION BY store_id ORDER BY quantity_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_store_id_max_quantity,
        SUM(quantity_whole) OVER (PARTITION BY product_id) AS product_id_sum_quantity_whole
    FROM deduped
),
scored AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        last_restock_label,
        quantity_whole,
        store_id_max_quantity,
        event_seq,
        event_seq_2,
        next_quantity,
        seq_no,
        rn,
        seq_no_rank,
        last_known_store_id_max_quantity,
        product_id_sum_quantity_whole,
        ROW_NUMBER() OVER (ORDER BY store_id_max_quantity, id) AS event_seq_3,
        AVG(quantity) OVER (PARTITION BY product_id) AS product_id_avg_quantity
    FROM with_windows_inventory
),
enriched AS (
    SELECT
        src.*,
        b.opened_date,
        b.city,
        b.region
    FROM scored src
    INNER JOIN stores b
        ON src.store_id = b.id
),
totals AS (
    SELECT
        CASE WHEN event_seq_2 >= 8 THEN 'LARGE' ELSE 'SMALL' END AS event_seq_2_tier,
        COUNT(*) FILTER (WHERE city = 'Singapore') AS singapore_rows,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list,
        COUNT(*) AS event_count,
        PERCENTILE_APPROX(store_id_max_quantity, 0.95) AS store_id_max_quantity_p95,
        SUM(CASE WHEN region = 'West' THEN quantity_whole ELSE 0 END) AS west_quantity_whole
    FROM enriched
    GROUP BY event_seq_2_tier
)
SELECT
    event_seq_2_tier,
    event_count,
    store_id_max_quantity_p95
FROM totals
ORDER BY store_id_max_quantity_p95;
