WITH metrics AS (
    SELECT
        id,
        order_id,
        unit_price,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY quantity NULLS FIRST) AS quantity_cume_dist,
        ROW_NUMBER() OVER (ORDER BY quantity, id) AS event_seq,
        LAST_VALUE(quantity) IGNORE NULLS OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity
    FROM order_items
    WHERE quantity = 7
),
base AS (
    SELECT
        id,
        quantity_cume_dist,
        last_known_quantity,
        CASE WHEN unit_price >= 330 THEN 'High' WHEN unit_price >= 130 THEN 'Medium' ELSE 'Low' END AS unit_price_tier
    FROM metrics
    WHERE order_id IN (16, 27, 30)
),
enriched AS (
    SELECT
        id,
        quantity_cume_dist,
        ROW_NUMBER() OVER (ORDER BY quantity_cume_dist DESC, id) AS event_seq,
        FIRST_VALUE(last_known_quantity, TRUE) OVER (ORDER BY last_known_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last_known_quantity,
        CUME_DIST() OVER (ORDER BY quantity_cume_dist) AS quantity_cume_dist_cume_dist,
        SUM(quantity_cume_dist) OVER (ORDER BY last_known_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity_cume_dist
    FROM base
),
base_order_items AS (
    SELECT
        id,
        quantity_cume_dist,
        event_seq,
        first_known_last_known_quantity,
        quantity_cume_dist_cume_dist,
        cumulative_quantity_cume_dist,
        cumulative_quantity_cume_dist - event_seq AS cumulative_quantity_cume_dist_minus_event_seq
    FROM enriched
    WHERE event_seq = 3
),
valid AS (
    SELECT *, quantity_cume_dist + event_seq AS quantity_cume_dist_plus_event_seq
    FROM base_order_items
    WHERE cumulative_quantity_cume_dist > 4
)
SELECT
    CASE WHEN cumulative_quantity_cume_dist >= 4 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_quantity_cume_dist_tier,
    CASE WHEN cumulative_quantity_cume_dist_minus_event_seq >= 86 THEN 'HIGH' ELSE 'LOW' END AS cumulative_quantity_cume_dist_minus_event_seq_tier,
    MAX(cumulative_quantity_cume_dist) AS max_cumulative_quantity_cume_dist,
    COUNT(*) AS event_count
FROM valid
GROUP BY CASE WHEN cumulative_quantity_cume_dist >= 4 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN cumulative_quantity_cume_dist_minus_event_seq >= 86 THEN 'HIGH' ELSE 'LOW' END;
