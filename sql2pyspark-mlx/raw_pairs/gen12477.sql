WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        DATE_TRUNC('week', last_restock) AS last_restock_period
    FROM inventory
    WHERE last_restock > TO_DATE('2023-03-04')
      AND YEAR(last_restock) = 2023
),
enriched AS (
    -- Attach stores attributes
    SELECT
        src.*,
        s.opened_date,
        s.store_name
    FROM cleaned src
    JOIN stores s
        ON src.store_id = s.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        MAX(quantity) OVER (PARTITION BY product_id, store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_quantity,
        COUNT(quantity) OVER (PARTITION BY product_id, store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_quantity,
        AVG(quantity) OVER (PARTITION BY product_id, store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_quantity
    FROM enriched
    WHERE store_id = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        running_count_quantity,
        running_avg_quantity,
        MAX(running_count_quantity) OVER w_grp AS running_max_running_count_quantity,
        SUM(running_count_quantity) OVER w_grp AS running_sum_running_count_quantity,
        AVG(running_count_quantity) OVER w_grp AS running_avg_running_count_quantity,
        SUM(running_max_quantity) OVER (PARTITION BY store_id) AS store_id_sum_running_max_quantity
    FROM scored
    WINDOW w_grp AS (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS quantity_avg_5,
        LEAD(store_id_sum_running_max_quantity, 2) OVER (ORDER BY running_sum_running_count_quantity, id) AS next_store_id_sum_running_max_quantity,
        AVG(running_max_running_count_quantity) OVER (ORDER BY running_avg_quantity, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS running_max_running_count_quantity_avg_5,
        NTILE(5) OVER (ORDER BY running_avg_quantity DESC, id) AS running_avg_quantity_bucket,
        RANK() OVER (PARTITION BY store_id ORDER BY quantity) AS quantity_rank
    FROM with_windows
)
-- Grouped metrics
SELECT
    CASE WHEN quantity >= 191 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    APPROX_PERCENTILE(next_store_id_sum_running_max_quantity, ARRAY(0.25, 0.5, 0.75)) AS next_store_id_sum_running_max_quantity_quartiles,
    APPROX_PERCENTILE(quantity_rank, ARRAY(0.25, 0.5, 0.75)) AS quantity_rank_quartiles,
    COUNT(*) AS num_rows,
    BIT_AND(quantity) AS quantity_bit_and,
    ROUND(AVG(running_sum_running_count_quantity), 2) AS avg_running_sum_running_count_quantity,
    MAX(next_store_id_sum_running_max_quantity) AS max_next_store_id_sum_running_max_quantity
FROM metrics
GROUP BY CASE WHEN quantity >= 191 THEN 'LARGE' ELSE 'SMALL' END
