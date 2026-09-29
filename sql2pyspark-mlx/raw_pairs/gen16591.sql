WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, MONTH(last_restock) AS last_restock_month
    FROM inventory
    WHERE last_restock BETWEEN '2023-04-27' AND '2023-09-10'
),
joined AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        e.quantity,
        e.last_restock,
        p.brand,
        p.stock_qty,
        p.launch_date
    FROM filtered e
    LEFT JOIN products p
        ON e.product_id = p.id
    WHERE p.cost > 580
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(stock_qty) OVER w_running AS prev_stock_qty,
        LEAD(stock_qty) OVER w_running AS next_stock_qty,
        LEAD(quantity) OVER w_running AS next_quantity,
        LAG(quantity) OVER w_running AS prev_quantity,
        ROW_NUMBER() OVER w_running AS event_seq,
        ROW_NUMBER() OVER w_ordered AS event_seq_2
    FROM joined
    WINDOW w_running AS (PARTITION BY brand ORDER BY last_restock, id), w_ordered AS (PARTITION BY MONTH(launch_date) ORDER BY launch_date DESC, id)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY quantity DESC, id) AS row_num
    FROM metrics
    QUALIFY row_num = 1
),
filtered_inventory AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(event_seq, 0) AS event_seq_filled
    FROM ranked
    WHERE brand IN ('Initech', 'Umbrella')
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM filtered_inventory
    QUALIFY RANK() OVER (PARTITION BY brand, product_id ORDER BY event_seq) = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY brand ORDER BY prev_quantity DESC NULLS LAST, id) = 1
)
SELECT
    id,
    quantity,
    last_restock,
    brand,
    stock_qty,
    prev_stock_qty,
    next_stock_qty,
    next_quantity,
    prev_quantity,
    CASE WHEN brand RLIKE '^(Initech|Globex)$' THEN 'CORE' WHEN brand RLIKE '^(Acme|Umbrella)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS brand_zone
FROM deduped
ORDER BY quantity DESC, prev_quantity;
