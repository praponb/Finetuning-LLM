WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        quantity,
        last_restock,
        RANK() OVER (PARTITION BY store_id ORDER BY quantity DESC) AS quantity_rank,
        LEAD(quantity, 2) OVER (PARTITION BY product_id ORDER BY last_restock, id) AS next_quantity,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('YEAR', last_restock) ORDER BY last_restock DESC, id) AS event_seq
    FROM inventory
),
metrics_inventory AS (
    -- Ranking and gap features
    SELECT
        id,
        quantity,
        last_restock,
        next_quantity,
        ROW_NUMBER() OVER (ORDER BY next_quantity DESC, id) AS row_seq,
        ROW_NUMBER() OVER (ORDER BY event_seq DESC, id) AS event_seq_2,
        AVG(next_quantity) OVER (PARTITION BY DATE_TRUNC('month', last_restock)) AS last_restock_period_avg_next_quantity,
        DENSE_RANK() OVER (ORDER BY quantity) AS quantity_dense_rank,
        SUM(event_seq) OVER (ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_event_seq
    FROM metrics
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM metrics_inventory
    INTERSECT ALL
    SELECT id
    FROM inventory
    WHERE product_id = 1
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics_inventory s
    JOIN matched_keys k ON s.id = k.id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        quantity,
        last_restock,
        next_quantity,
        last_restock_period_avg_next_quantity,
        quantity_dense_rank,
        cumulative_event_seq,
        GREATEST(row_seq, quantity) AS greatest_row_seq_quantity,
        quantity - next_quantity AS quantity_minus_next_quantity
    FROM kept_rows
    WHERE next_quantity <= 88
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN greatest_row_seq_quantity >= 74 THEN 'HIGH' ELSE 'LOW' END AS greatest_row_seq_quantity_tier,
        PERCENTILE_APPROX(cumulative_event_seq, 0.9) AS cumulative_event_seq_p90,
        ROUND(AVG(quantity_minus_next_quantity), 2) AS avg_quantity_minus_next_quantity
    FROM base
    GROUP BY 1
    HAVING ROUND(AVG(quantity_minus_next_quantity), 2) >= 0
)
SELECT greatest_row_seq_quantity_tier, cumulative_event_seq_p90
FROM summary
ORDER BY greatest_row_seq_quantity_tier DESC
LIMIT 5
