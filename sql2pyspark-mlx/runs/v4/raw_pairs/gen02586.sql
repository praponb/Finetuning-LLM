WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        customer_id,
        ship_to,
        attributes['gift'] AS gift,
        ARRAY_JOIN(ARRAY_SORT(coupon_codes), ',') AS coupons_text,
        COALESCE(ship_to.country, 'UNKNOWN') AS country_or_unknown,
        ARRAY_MAX(TRANSFORM(items, i -> i.price)) AS max_item_price
    FROM carts
    WHERE customer_id IN (20, 33, 40)
)
-- Running and moving aggregates
SELECT
    *,
    LEAD(max_item_price, 2) OVER w_recent AS next_max_item_price,
    NTILE(5) OVER w_recent AS max_item_price_bucket,
    ROUND(max_item_price / SUM(max_item_price) OVER (PARTITION BY customer_id), 4) AS max_item_price_share
FROM flattened_attrs
WINDOW w_recent AS (PARTITION BY customer_id ORDER BY max_item_price, id);
