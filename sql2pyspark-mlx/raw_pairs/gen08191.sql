WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_part AS seq_no,
        LAG(quantity, 1) OVER w_part AS prev_quantity
    FROM inventory
    WINDOW w_part AS (ORDER BY last_restock, id)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(prev_quantity) OVER w_grp AS product_id_max_prev_quantity,
        AVG(prev_quantity) OVER w_grp AS product_id_avg_prev_quantity,
        LEAD(quantity, 1, 0) OVER (PARTITION BY product_id ORDER BY last_restock, id) AS next_quantity,
        AVG(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS quantity_avg_2
    FROM scored
    WHERE prev_quantity >= 156
    WINDOW w_grp AS (PARTITION BY product_id)
),
metrics_inventory AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        last_restock,
        next_quantity,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY next_quantity DESC, id) AS seq_no_2,
        LAG(product_id_avg_prev_quantity, 2, 0) OVER (PARTITION BY product_id, store_id ORDER BY product_id_avg_prev_quantity, id) AS prev_product_id_avg_prev_quantity,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY store_id), 4) AS seq_no_share
    FROM metrics
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY next_quantity DESC, id) AS dedup_rank
        FROM metrics_inventory
    ) x
    WHERE dedup_rank = 1
),
enriched AS (
    -- Attach stores attributes
    SELECT src.*, d.opened_date
    FROM deduped src
    LEFT JOIN stores d
        ON src.store_id = d.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        opened_date,
        SUM(seq_no_2) OVER (PARTITION BY store_id ORDER BY seq_no_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_seq_no_2,
        COUNT(seq_no_2) OVER (PARTITION BY store_id ORDER BY seq_no_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_seq_no_2,
        AVG(seq_no_2) OVER (PARTITION BY store_id ORDER BY seq_no_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_seq_no_2,
        MAX(seq_no_2) OVER (PARTITION BY store_id ORDER BY seq_no_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_seq_no_2,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY dedup_rank DESC, id) AS event_seq
    FROM enriched
)
-- Grouped metrics
SELECT
    CASE WHEN event_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END AS event_seq_tier,
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(event_seq, 0.95) AS event_seq_p95,
    APPROX_PERCENTILE(event_seq, ARRAY(0.25, 0.5, 0.75)) AS event_seq_quartiles,
    APPROX_PERCENTILE(event_seq, ARRAY(0.25, 0.5, 0.75)) AS event_seq_quartiles_2,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN event_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END ORDER BY COUNT(DISTINCT id) DESC) AS distinct_ids_percent_rank
FROM with_windows
GROUP BY CASE WHEN event_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('YEAR', opened_date), DATE_TRUNC('WEEK', opened_date);
