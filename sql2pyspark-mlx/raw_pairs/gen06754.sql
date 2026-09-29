WITH cleaned AS (
    SELECT *, ROUND(quantity * 0.85, 2) AS quantity_scaled
    FROM order_items
    WHERE product_id IS NOT NULL
      AND order_id = 31
),
enriched AS (
    SELECT
        id,
        quantity,
        unit_price,
        LAG(quantity_scaled) OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC) AS prev_quantity_scaled,
        LEAD(quantity_scaled) OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC) AS next_quantity_scaled,
        LAG(quantity) OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC) AS prev_quantity,
        LEAD(quantity) OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC) AS next_quantity,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC) AS event_seq
    FROM cleaned
),
cleaned_order_items AS (
    SELECT
        id,
        quantity,
        prev_quantity_scaled,
        next_quantity_scaled,
        prev_quantity,
        next_quantity,
        event_seq,
        LEAST(prev_quantity_scaled, quantity) AS least_prev_quantity_scaled_quantity
    FROM enriched
    WHERE prev_quantity_scaled < 42
),
metrics AS (
    SELECT
        id,
        quantity,
        prev_quantity_scaled,
        prev_quantity,
        next_quantity,
        SUM(quantity) OVER (ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_quantity,
        MAX(quantity) OVER (ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_quantity,
        AVG(quantity) OVER (ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_quantity,
        COUNT(quantity) OVER (ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_quantity
    FROM cleaned_order_items
)
SELECT
    CASE WHEN running_max_quantity >= 8.6 THEN 'HIGH' ELSE 'LOW' END AS running_max_quantity_tier,
    PERCENTILE_APPROX(quantity, 0.5) AS quantity_p50,
    APPROX_PERCENTILE(prev_quantity_scaled, 0.95) AS prev_quantity_scaled_p95,
    COUNT(*) AS num_rows
FROM metrics
GROUP BY 1
ORDER BY quantity_p50 DESC NULLS FIRST;
