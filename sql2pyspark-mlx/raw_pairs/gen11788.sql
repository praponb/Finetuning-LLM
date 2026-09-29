WITH enriched AS (
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        AVG(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_avg_quantity,
        MIN(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_min_quantity,
        MAX(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_max_quantity,
        SUM(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_sum_quantity,
        AVG(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) quantity_avg_3_prior
    FROM inventory
),
enriched_inventory AS (
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        s.last_restock,
        s.last7_avg_quantity,
        s.last7_min_quantity,
        s.quantity_avg_3_prior,
        b.opened_date,
        b.store_name
    FROM enriched s
    INNER JOIN stores b
        ON s.store_id = b.id
),
metrics AS (
    SELECT
        *,
        LAG(last7_min_quantity, 1) OVER (PARTITION BY product_id ORDER BY opened_date, id) prev_last7_min_quantity,
        LEAD(last7_avg_quantity, 1, 0) OVER (PARTITION BY MONTH(opened_date), store_id ORDER BY last_restock, id) next_last7_avg_quantity,
        SUM(last7_avg_quantity) OVER (PARTITION BY product_id ORDER BY last7_avg_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_last7_avg_quantity,
        SUM(last7_min_quantity) OVER (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_last7_min_quantity,
        NTILE(5) OVER (PARTITION BY MONTH(opened_date) ORDER BY last7_min_quantity DESC, id) last7_min_quantity_bucket
    FROM enriched_inventory
),
metrics_inventory AS (
    SELECT
        id,
        store_id,
        last_restock,
        last7_avg_quantity,
        quantity_avg_3_prior,
        opened_date,
        next_last7_avg_quantity,
        cumulative_last7_avg_quantity,
        running_last7_min_quantity,
        AVG(cumulative_last7_avg_quantity) OVER (PARTITION BY DATE_TRUNC('month', last_restock), store_id ORDER BY quantity_avg_3_prior, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) cumulative_last7_avg_quantity_avg_3,
        SUM(quantity_avg_3_prior) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_quantity_avg_3_prior
    FROM metrics
),
scored AS (
    SELECT
        *,
        SUM(next_last7_avg_quantity) OVER (ORDER BY quantity_avg_3_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_next_last7_avg_quantity,
        LAG(cumulative_last7_avg_quantity, 1) OVER (PARTITION BY YEAR(last_restock), store_id ORDER BY next_last7_avg_quantity, id) prev_cumulative_last7_avg_quantity,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('month', opened_date), store_id ORDER BY running_last7_min_quantity DESC NULLS LAST) running_last7_min_quantity_dense_rank,
        ROW_NUMBER() OVER (ORDER BY running_last7_min_quantity, id) event_seq,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC, id) event_seq_2
    FROM metrics_inventory
),
cleaned AS (
    SELECT
        id,
        store_id,
        last_restock,
        last7_avg_quantity,
        quantity_avg_3_prior,
        opened_date,
        next_last7_avg_quantity,
        cumulative_last7_avg_quantity,
        cumulative_quantity_avg_3_prior,
        prev_cumulative_last7_avg_quantity,
        running_last7_min_quantity_dense_rank,
        event_seq_2,
        LAST_DAY(last_restock) last_restock_month_end
    FROM scored
    WHERE cumulative_last7_avg_quantity BETWEEN 668 AND 1075
)
SELECT
    p.city,
    COUNT(*) AS num_rows,
    MAX(src.event_seq_2) AS max_event_seq_2,
    SUM(src.last7_avg_quantity) AS total_last7_avg_quantity
FROM cleaned src
INNER JOIN stores p ON src.store_id = p.id
GROUP BY p.city
