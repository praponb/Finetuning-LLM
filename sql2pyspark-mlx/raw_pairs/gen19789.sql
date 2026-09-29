WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        CASE WHEN quantity >= 260 THEN 'High' WHEN quantity >= 100 THEN 'Medium' ELSE 'Low' END AS quantity_level
    FROM inventory
    WHERE last_restock > TO_DATE('2023-03-31')
      AND quantity BETWEEN 98 AND 127
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY product_id, store_id ORDER BY quantity DESC, id) AS quantity_bucket,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock, id) AS seq_no
    FROM valid
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(quantity_bucket) OVER w_running AS last4_min_quantity_bucket,
        MAX(quantity_bucket) OVER w_running AS last4_max_quantity_bucket,
        COUNT(quantity_bucket) OVER w_running AS last4_count_quantity_bucket,
        SUM(quantity_bucket) OVER w_running AS last4_sum_quantity_bucket,
        NTILE(3) OVER (PARTITION BY product_id, store_id ORDER BY row_seq, id) AS row_seq_bucket
    FROM enriched
    WINDOW w_running AS (PARTITION BY product_id ORDER BY row_seq, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
valid_inventory AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        quantity_level,
        quantity_bucket,
        row_seq,
        seq_no,
        last4_count_quantity_bucket,
        last4_sum_quantity_bucket,
        row_seq_bucket,
        CEIL(row_seq_bucket) AS row_seq_bucket_whole
    FROM metrics
    WHERE row_seq_bucket BETWEEN 5 AND 7
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT store_id
    FROM valid_inventory
    INTERSECT
    SELECT store_id
    FROM inventory
    WHERE quantity < 68
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid_inventory s
    INNER JOIN remaining_keys k ON s.store_id = k.store_id
)
SELECT
    s.id,
    s.store_id,
    s.quantity_level,
    s.quantity_bucket,
    s.row_seq,
    s.seq_no,
    s.last4_sum_quantity_bucket,
    s.row_seq_bucket,
    s.row_seq_bucket_whole,
    (SELECT MIN(t2.amount) FROM orders t2 WHERE t2.store_id = s.store_id) AS min_order_amount
FROM reconciled s;
