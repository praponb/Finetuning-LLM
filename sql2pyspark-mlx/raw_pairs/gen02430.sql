WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        discount - amount discount_minus_amount,
        COALESCE(channel, 'UNKNOWN') channel_filled
    FROM orders
    WHERE store_id IS NOT NULL
      AND YEAR(order_date) >= 2024
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.order_item_unit_price
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT store_id, status
    FROM with_latest
    INTERSECT ALL
    SELECT store_id, status
    FROM orders
    WHERE discount >= 0.16
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_latest s
    JOIN remaining_keys k ON s.store_id = k.store_id AND s.status = k.status
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM kept_rows s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.order_date BETWEEN pr.starts_on AND pr.ends_on
)
SELECT
    id,
    status,
    discount,
    shipping_fee,
    discount_minus_amount,
    channel_filled,
    order_item_unit_price,
    promo_id,
    promo_code
FROM attributed
