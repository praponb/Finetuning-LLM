WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        coupon_codes,
        items,
        ship_to,
        ARRAY_MAX(TRANSFORM(items, i -> i.price)) AS max_item_price,
        ARRAY_JOIN(ARRAY_SORT(coupon_codes), ',') AS coupons_text
    FROM carts
)
-- Window metrics over several different partitions
SELECT
    id,
    max_item_price,
    coupons_text,
    SUM(max_item_price) OVER (ORDER BY max_item_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_max_item_price,
    MAX(max_item_price) OVER (ORDER BY max_item_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_max_item_price,
    AVG(max_item_price) OVER (ORDER BY max_item_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_max_item_price
FROM flattened_attrs;
