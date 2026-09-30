WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM order_items
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, quantity + unit_price AS line_total
    FROM deduped
    WHERE order_id IN (15, 19, 32)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(line_total) OVER w_hist AS running_min_line_total,
        AVG(line_total) OVER w_hist AS running_avg_line_total,
        MAX(line_total) OVER w_hist AS running_max_line_total
    FROM cleaned
    WHERE quantity > 6
    WINDOW w_hist AS (PARTITION BY product_id ORDER BY line_total, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_order_items AS (
    -- Attach orders attributes
    SELECT
        e.*,
        d.order_date,
        d.status
    FROM enriched e
    JOIN orders d
        ON e.order_id = d.id
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN running_avg_line_total >= 69 THEN 'LARGE' ELSE 'SMALL' END AS running_avg_line_total_tier,
        product_id,
        order_id,
        ROUND(AVG(running_max_line_total), 2) AS avg_running_max_line_total,
        PERCENTILE_APPROX(running_avg_line_total, 0.5) AS running_avg_line_total_p50,
        MIN(unit_price) AS min_unit_price
    FROM enriched_order_items
    GROUP BY GROUPING SETS ((CASE WHEN running_avg_line_total >= 69 THEN 'LARGE' ELSE 'SMALL' END, product_id, order_id), (CASE WHEN running_avg_line_total >= 69 THEN 'LARGE' ELSE 'SMALL' END, product_id), (CASE WHEN running_avg_line_total >= 69 THEN 'LARGE' ELSE 'SMALL' END))
    HAVING min_unit_price >= 0
)
SELECT
    s.product_id,
    s.order_id,
    s.avg_running_max_line_total,
    s.running_avg_line_total_p50,
    s.min_unit_price,
    (SELECT AVG(t2.cost) FROM shipments t2 WHERE t2.order_id = s.order_id) AS avg_shipment_cost
FROM stats s;
