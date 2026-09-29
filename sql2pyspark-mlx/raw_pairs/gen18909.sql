WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY quantity DESC) AS quantity_rank,
        LAG(quantity, 1) OVER (PARTITION BY store_id ORDER BY quantity ASC, id ASC) AS prev_quantity,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity ASC, id ASC) AS event_seq,
        SUM(quantity) OVER (PARTITION BY MONTH(last_restock), store_id ORDER BY last_restock ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity,
        MAX(quantity) OVER (PARTITION BY MONTH(last_restock), store_id) AS last_restock_month_max_quantity
    FROM inventory
    WHERE quantity = 238
),
enriched AS (
    -- Attach products attributes
    SELECT
        a.*,
        p.brand,
        p.launch_date,
        p.stock_qty
    FROM scored a
    LEFT JOIN products p
        ON a.product_id = p.id
),
enriched_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(prev_quantity, 1) OVER (ORDER BY last_restock ASC, id ASC) AS next_prev_quantity,
        FIRST_VALUE(running_quantity) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY running_quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_quantity,
        AVG(quantity) OVER w_running AS quantity_avg_3,
        AVG(last_restock_month_max_quantity) OVER w_recent AS last_restock_month_max_quantity_avg_6_prior,
        SUM(running_quantity) OVER (ORDER BY prev_quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_quantity
    FROM enriched
    WINDOW w_running AS (PARTITION BY store_id ORDER BY event_seq ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_recent AS (ORDER BY launch_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        quantity,
        last_restock,
        quantity_rank,
        event_seq,
        running_quantity,
        last_restock_month_max_quantity,
        launch_date,
        stock_qty,
        next_prev_quantity,
        first_known_running_quantity,
        quantity_avg_3,
        last_restock_month_max_quantity_avg_6_prior,
        running_running_quantity,
        event_seq / NULLIF(next_prev_quantity, 0) AS event_seq_per_next_prev_quantity
    FROM enriched_inventory
    WHERE brand IS DISTINCT FROM 'Acme'
),
enriched_inventory_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(event_seq) OVER w_ordered AS cumulative_event_seq,
        ROW_NUMBER() OVER (ORDER BY running_running_quantity DESC, id ASC) AS seq_no
    FROM base
    WINDOW w_ordered AS (ORDER BY last_restock_month_max_quantity_avg_6_prior ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY cumulative_event_seq DESC NULLS LAST) AS cumulative_event_seq_dense_rank,
        AVG(running_running_quantity) OVER w_recent AS running_running_quantity_avg_2_prior,
        ROW_NUMBER() OVER (PARTITION BY MONTH(launch_date) ORDER BY quantity ASC, id ASC) AS row_seq,
        AVG(quantity_avg_3) OVER (ORDER BY last_restock_month_max_quantity_avg_6_prior ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS quantity_avg_3_avg_3,
        SUM(quantity_rank) OVER w_recent_2 AS running_quantity_rank
    FROM enriched_inventory_2
    WINDOW w_recent AS (PARTITION BY MONTH(last_restock) ORDER BY last_restock_month_max_quantity ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), w_recent_2 AS (ORDER BY last_restock ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    seq_no,
    CASE WHEN last_restock_month_max_quantity >= 198 THEN 'LARGE' ELSE 'SMALL' END AS last_restock_month_max_quantity_tier,
    MIN(row_seq) AS min_row_seq,
    BIT_OR(quantity) AS quantity_bit_or,
    MIN(seq_no) AS min_seq_no,
    COUNT(*) AS num_rows
FROM metrics
GROUP BY seq_no, CASE WHEN last_restock_month_max_quantity >= 198 THEN 'LARGE' ELSE 'SMALL' END
HAVING quantity_bit_or >= 1
ORDER BY seq_no DESC NULLS FIRST;
