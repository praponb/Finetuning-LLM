WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        unit_price + quantity AS line_total,
        CAST(quantity AS DOUBLE) AS quantity_str
    FROM order_items
    WHERE order_id IN (8, 35)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        unit_price,
        line_total,
        quantity_str
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id ASC) AS row_num
        FROM cleaned
    ) t
    WHERE row_num = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(quantity_str) OVER (ORDER BY line_total ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity_str,
        SUM(line_total) OVER w_ordered AS cumulative_line_total
    FROM deduped
    WHERE unit_price > 280
    WINDOW w_ordered AS (ORDER BY unit_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        line_total,
        quantity_str,
        cumulative_line_total,
        LEAD(quantity_str) OVER (ORDER BY line_total ASC, id ASC) AS next_quantity_str,
        LAG(quantity_str) OVER (ORDER BY line_total ASC, id ASC) AS prev_quantity_str,
        LAG(cumulative_line_total) OVER (ORDER BY line_total ASC, id ASC) AS prev_cumulative_line_total,
        ROW_NUMBER() OVER (ORDER BY line_total ASC, id ASC) AS event_seq
    FROM with_windows
)
-- Multi-level aggregation
SELECT
    CASE WHEN prev_cumulative_line_total >= 326 THEN 'HIGH' ELSE 'LOW' END AS prev_cumulative_line_total_tier,
    ROUND(AVG(next_quantity_str), 2) AS avg_next_quantity_str,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM enriched
GROUP BY CASE WHEN prev_cumulative_line_total >= 326 THEN 'HIGH' ELSE 'LOW' END
ORDER BY distinct_ids DESC;
