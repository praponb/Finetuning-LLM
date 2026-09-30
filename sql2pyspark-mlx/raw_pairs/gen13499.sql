WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity, id) AS seq_no,
        STDDEV_SAMP(quantity) OVER w_part AS last_restock_month_stddev_samp_quantity,
        AVG(quantity) OVER (ORDER BY last_restock, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS quantity_avg_2
    FROM inventory
    WINDOW w_part AS (PARTITION BY MONTH(last_restock), product_id)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        seq_no,
        last_restock_month_stddev_samp_quantity,
        quantity_avg_2,
        CEIL(last_restock_month_stddev_samp_quantity) AS last_restock_month_stddev_samp_quantity_whole
    FROM with_windows
    WHERE last_restock > DATE '2023-09-14'
      AND last_restock BETWEEN '2024-05-25' AND '2024-06-01'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        quantity,
        last_restock_month_stddev_samp_quantity_whole,
        ROUND(seq_no, 1) AS seq_no_rounded
    FROM base
    WHERE quantity_avg_2 != 193
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, product_id
    FROM filtered
    INTERSECT ALL
    SELECT id, product_id
    FROM inventory
    WHERE last_restock >= '2025-09-02'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.id = k.id AND s.product_id = k.product_id
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        product_id,
        COUNT(*) AS event_count,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(AVG(seq_no_rounded), 2) AS avg_seq_no_rounded,
        ROUND(AVG(quantity), 2) AS avg_quantity
    FROM verified
    GROUP BY product_id
)
SELECT
    s.product_id,
    s.event_count,
    s.distinct_ids,
    s.avg_seq_no_rounded,
    s.avg_quantity,
    (SELECT AVG(x.revenue) FROM store_sales x WHERE x.product_id = s.product_id) AS avg_store_sale_revenue
FROM summary s;
