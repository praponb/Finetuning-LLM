WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        CAST(quantity AS STRING) AS quantity_num,
        DATE_TRUNC('year', last_restock) AS last_restock_period
    FROM inventory
    WHERE last_restock > TO_DATE('2023-03-17')
      AND last_restock > '2023-09-01'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        last_restock,
        quantity_num,
        last_restock_period,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock_period DESC, id) AS seq_no
    FROM valid
    WHERE store_id IN (1, 2, 3, 5)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(seq_no) OVER w_grp AS running_avg_seq_no,
        SUM(seq_no) OVER w_grp AS running_sum_seq_no,
        COUNT(seq_no) OVER w_grp AS running_count_seq_no,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY MONTH(last_restock)), 4) AS seq_no_share
    FROM with_windows
    WINDOW w_grp AS (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        last_restock,
        quantity_num,
        seq_no,
        running_count_seq_no,
        AVG(running_count_seq_no) OVER (ORDER BY seq_no_share, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS running_count_seq_no_avg_5_prior,
        LAST_VALUE(running_avg_seq_no, TRUE) OVER (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_avg_seq_no
    FROM enriched
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        last_restock,
        quantity_num,
        seq_no,
        running_count_seq_no,
        running_count_seq_no_avg_5_prior,
        CAST(running_count_seq_no_avg_5_prior AS DOUBLE) AS running_count_seq_no_avg_5_prior_num
    FROM metrics
    WHERE seq_no BETWEEN 5 AND 6
),
filtered_inventory AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        last_restock,
        quantity_num,
        seq_no,
        running_count_seq_no,
        running_count_seq_no_avg_5_prior,
        running_count_seq_no_avg_5_prior_num,
        DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_label,
        DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
    FROM filtered
    WHERE last_restock >= DATE '2023-08-13'
)
SELECT
    id,
    last_restock,
    quantity_num,
    running_count_seq_no_avg_5_prior_num,
    last_restock_label,
    days_since_last_restock
FROM filtered_inventory
ORDER BY id DESC
