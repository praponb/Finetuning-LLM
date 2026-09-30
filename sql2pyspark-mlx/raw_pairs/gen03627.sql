WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, FLOOR(quantity) AS quantity_whole
    FROM inventory
    WHERE last_restock >= DATE '2023-03-06'
),
joined AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.product_id,
        a.quantity,
        a.last_restock,
        a.quantity_whole,
        p.brand,
        p.launch_date
    FROM filtered a
    LEFT JOIN products p
        ON a.product_id = p.id
    WHERE p.stock_qty BETWEEN 226 AND 385
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY MONTH(last_restock) ORDER BY quantity DESC, id) AS seq_no,
        SUM(quantity) OVER (PARTITION BY YEAR(launch_date) ORDER BY quantity_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity,
        COUNT(quantity) OVER (PARTITION BY product_id) AS product_id_count_quantity,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', launch_date), product_id ORDER BY launch_date, id) AS event_seq
    FROM joined
),
enriched_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(seq_no, TRUE) OVER (PARTITION BY brand ORDER BY quantity_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_seq_no,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY event_seq, id) AS row_seq,
        AVG(event_seq) OVER (PARTITION BY product_id) AS product_id_avg_event_seq,
        SUM(running_quantity) OVER (PARTITION BY product_id ORDER BY quantity_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_quantity,
        SUM(event_seq) OVER (PARTITION BY MONTH(launch_date) ORDER BY product_id_count_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_event_seq
    FROM enriched
),
joined_inventory AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        e.last_restock,
        e.quantity_whole,
        e.launch_date,
        e.product_id_count_quantity,
        e.event_seq,
        e.row_seq,
        e.product_id_avg_event_seq,
        e.cumulative_running_quantity,
        e.cumulative_event_seq,
        p.category,
        p.price
    FROM enriched_inventory e
    INNER JOIN products p
        ON e.product_id = p.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM joined_inventory
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category, product_id ORDER BY event_seq DESC NULLS FIRST, id) = 1
)
SELECT
    id,
    last_restock,
    quantity_whole,
    launch_date,
    product_id_count_quantity,
    event_seq,
    row_seq,
    product_id_avg_event_seq,
    cumulative_event_seq
FROM latest
ORDER BY quantity_whole DESC NULLS LAST, launch_date;
