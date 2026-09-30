WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        created_at,
        items,
        SIZE(items) AS num_items,
        ship_to.city AS ship_city,
        ship_to.zip AS ship_zip,
        attributes['gift'] AS gift,
        EXISTS(items, x -> x.price > 100) AS has_premium_item
    FROM carts
)
-- Window metrics over several different partitions
SELECT
    id,
    created_at,
    items,
    num_items,
    ship_city,
    ship_zip,
    gift,
    has_premium_item,
    AVG(num_items) OVER (PARTITION BY ship_city) AS ship_city_avg_num_items,
    PERCENT_RANK() OVER (PARTITION BY ship_city ORDER BY num_items) AS num_items_percent_rank,
    AVG(num_items) OVER (PARTITION BY ship_city ORDER BY num_items, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS num_items_avg_6_prior,
    LEAD(num_items, 1) OVER (PARTITION BY ship_city ORDER BY created_at, id) AS next_num_items,
    LAG(num_items, 1) OVER (PARTITION BY ship_city ORDER BY num_items, id) AS prev_num_items
FROM flattened_attrs;
