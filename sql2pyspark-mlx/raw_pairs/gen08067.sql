WITH parsed AS (
    SELECT
        id,
        ship_to,
        SIZE(items) AS num_items,
        attributes['utm_source'] AS utm_source,
        EXISTS(items, i -> i.price > 100) AS has_premium_item
    FROM carts
)
SELECT
    *,
    COUNT(num_items) OVER w_ordered AS last3_count_num_items,
    MAX(num_items) OVER w_ordered AS last3_max_num_items,
    SUM(num_items) OVER w_ordered AS last3_sum_num_items,
    AVG(num_items) OVER w_ordered AS last3_avg_num_items
FROM parsed
WHERE has_premium_item
WINDOW w_ordered AS (ORDER BY num_items, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
