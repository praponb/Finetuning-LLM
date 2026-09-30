WITH parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        customer_id,
        items,
        AGGREGATE(items, 0.0D, (acc, li) -> acc + li.qty * li.price, acc -> ROUND(acc, 2)) AS items_total,
        SIZE(FILTER(items, li -> li.qty >= 3)) AS bulk_lines,
        attributes['device'] AS device
    FROM carts
)
-- Window metrics over several different partitions
SELECT
    id,
    device,
    COUNT(items_total) OVER (PARTITION BY customer_id ORDER BY bulk_lines, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_items_total,
    MAX(items_total) OVER (PARTITION BY customer_id ORDER BY bulk_lines, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_items_total,
    AVG(items_total) OVER (PARTITION BY customer_id ORDER BY bulk_lines, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_items_total,
    AVG(bulk_lines) OVER (PARTITION BY customer_id ORDER BY items_total, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS bulk_lines_avg_5_prior
FROM parsed;
