WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(quantity) OVER (PARTITION BY store_id, product_id ORDER BY last_restock ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_quantity,
        AVG(quantity) OVER (PARTITION BY store_id, product_id ORDER BY last_restock ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_quantity,
        SUM(quantity) OVER (PARTITION BY store_id, product_id ORDER BY last_restock ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_quantity
    FROM inventory
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(quantity) OVER w_part AS last3_min_quantity,
        AVG(quantity) OVER w_part AS last3_avg_quantity,
        MAX(quantity) OVER w_part AS last3_max_quantity,
        MAX(running_sum_quantity) OVER w_grp AS product_id_max_running_sum_quantity
    FROM with_windows
    WINDOW w_part AS (PARTITION BY store_id ORDER BY running_max_quantity ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY product_id)
),
enriched AS (
    -- Attach products attributes
    SELECT s.*, p.brand
    FROM scored s
    LEFT JOIN products p
        ON s.product_id = p.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        running_avg_quantity,
        last3_avg_quantity,
        last3_max_quantity,
        brand,
        LEAD(running_avg_quantity) OVER w_ordered AS next_running_avg_quantity,
        LAG(running_avg_quantity) OVER w_ordered AS prev_running_avg_quantity,
        LEAD(quantity) OVER w_ordered AS next_quantity,
        ROW_NUMBER() OVER w_ordered AS row_seq
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY store_id ORDER BY running_sum_quantity ASC, id ASC)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY brand, product_id ORDER BY prev_running_avg_quantity DESC NULLS FIRST, id ASC) = 1
),
enriched_inventory AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(prev_running_avg_quantity) OVER (PARTITION BY YEAR(last_restock), brand ORDER BY next_running_avg_quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_running_avg_quantity,
        FIRST_VALUE(last3_max_quantity) IGNORE NULLS OVER (PARTITION BY brand ORDER BY row_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last3_max_quantity,
        AVG(next_running_avg_quantity) OVER (PARTITION BY store_id ORDER BY row_seq ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS next_running_avg_quantity_avg_3_prior
    FROM latest
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', last_restock) AS quarter_start,
    CASE WHEN cumulative_prev_running_avg_quantity >= 926 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_prev_running_avg_quantity_tier,
    AVG(first_known_last3_max_quantity) AS avg_first_known_last3_max_quantity,
    ROUND(AVG(last3_max_quantity), 2) AS avg_last3_max_quantity
FROM enriched_inventory
GROUP BY CUBE (DATE_TRUNC('QUARTER', last_restock), CASE WHEN cumulative_prev_running_avg_quantity >= 926 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY avg_first_known_last3_max_quantity DESC NULLS LAST
