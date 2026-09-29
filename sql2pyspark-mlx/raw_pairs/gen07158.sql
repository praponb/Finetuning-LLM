WITH valid AS (
    SELECT
        id,
        quantity,
        last_restock,
        CASE WHEN quantity >= 183 THEN 'High' WHEN quantity >= 42 THEN 'Medium' ELSE 'Low' END AS quantity_band
    FROM inventory
    WHERE last_restock >= DATE '2023-05-28'
      AND product_id IN (2, 4, 6, 8)
),
enriched AS (
    SELECT
        id,
        quantity,
        last_restock,
        NTILE(4) OVER (ORDER BY quantity DESC, id) AS quantity_bucket,
        SUM(quantity) OVER (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        SUM(quantity) OVER (PARTITION BY YEAR(last_restock)) AS last_restock_year_sum_quantity
    FROM valid
),
scored AS (
    SELECT
        *,
        LEAD(quantity_bucket) OVER w_running AS next_quantity_bucket,
        LAG(quantity_bucket) OVER w_running AS prev_quantity_bucket,
        LEAD(last_restock_year_sum_quantity) OVER w_running AS next_last_restock_year_sum_quantity,
        LAG(last_restock_year_sum_quantity) OVER w_running AS prev_last_restock_year_sum_quantity,
        ROW_NUMBER() OVER w_running AS event_seq,
        ROW_NUMBER() OVER (ORDER BY last_restock_year_sum_quantity, id) AS row_seq
    FROM enriched
    WINDOW w_running AS (ORDER BY last_restock, id)
),
scored_inventory AS (
    SELECT
        id,
        quantity,
        quantity_bucket,
        last_restock_year_sum_quantity,
        prev_quantity_bucket,
        next_last_restock_year_sum_quantity,
        prev_last_restock_year_sum_quantity,
        event_seq,
        row_seq,
        MIN(last_restock_year_sum_quantity) OVER (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_last_restock_year_sum_quantity,
        MAX(last_restock_year_sum_quantity) OVER (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_last_restock_year_sum_quantity,
        AVG(last_restock_year_sum_quantity) OVER (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_last_restock_year_sum_quantity,
        COUNT(last_restock_year_sum_quantity) OVER (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_last_restock_year_sum_quantity,
        NTILE(3) OVER (PARTITION BY YEAR(last_restock) ORDER BY event_seq, id) AS event_seq_bucket
    FROM scored
),
base AS (
    SELECT
        id,
        quantity,
        quantity_bucket,
        last_restock_year_sum_quantity,
        prev_quantity_bucket,
        next_last_restock_year_sum_quantity,
        prev_last_restock_year_sum_quantity,
        event_seq,
        row_seq,
        running_min_last_restock_year_sum_quantity,
        running_max_last_restock_year_sum_quantity,
        running_count_last_restock_year_sum_quantity,
        event_seq_bucket,
        row_seq / NULLIF(last_restock_year_sum_quantity, 0) AS row_seq_per_last_restock_year_sum_quantity
    FROM scored_inventory
    WHERE quantity <= 204
),
valid_inventory AS (
    SELECT
        *,
        running_count_last_restock_year_sum_quantity * row_seq_per_last_restock_year_sum_quantity AS running_count_last_restock_year_sum_quantity_times_row_seq_per_last_restock_year_sum_quantity,
        ROUND(next_last_restock_year_sum_quantity, 1) AS next_last_restock_year_sum_quantity_rounded
    FROM base
    WHERE quantity != 207
)
SELECT
    id,
    quantity,
    quantity_bucket,
    prev_quantity_bucket,
    event_seq,
    running_min_last_restock_year_sum_quantity,
    running_max_last_restock_year_sum_quantity,
    running_count_last_restock_year_sum_quantity,
    event_seq_bucket
FROM valid_inventory
ORDER BY id DESC NULLS LAST;
