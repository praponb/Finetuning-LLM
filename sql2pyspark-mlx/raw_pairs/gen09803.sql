WITH flattened_attrs AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        attributes,
        ship_to.country AS ship_country,
        ARRAY_MAX(TRANSFORM(items, x -> x.price)) AS max_item_price
    FROM carts
    WHERE customer_id IS NOT NULL
)
SELECT
    id,
    customer_id,
    ship_country,
    max_item_price,
    SUM(max_item_price) OVER w_part AS running_sum_max_item_price,
    COUNT(max_item_price) OVER w_part AS running_count_max_item_price,
    MIN(max_item_price) OVER w_part AS running_min_max_item_price,
    ROW_NUMBER() OVER (ORDER BY max_item_price DESC, id) AS seq_no
FROM flattened_attrs
WINDOW w_part AS (PARTITION BY customer_id, ship_country ORDER BY max_item_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
