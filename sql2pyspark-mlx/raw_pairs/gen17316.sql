WITH with_latest AS (
    SELECT s.*, x.order_shipping_fee
    FROM stores s
    JOIN LATERAL (
            SELECT c.shipping_fee AS order_shipping_fee
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date, id) AS row_num
    FROM with_latest
    QUALIFY row_num = 1
),
with_last AS (
    SELECT
        s.id,
        s.city,
        s.sqft,
        s.order_shipping_fee,
        top1.inventory_quantity,
        top1.inventory_last_restock
    FROM deduped s
    JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
deduped_stores AS (
    SELECT
        id,
        city,
        sqft,
        order_shipping_fee,
        inventory_last_restock
    FROM with_last
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC NULLS FIRST, id) = 1
),
base AS (
    SELECT
        *,
        CASE WHEN order_shipping_fee >= 37 THEN 'High' WHEN order_shipping_fee >= 5 THEN 'Medium' ELSE 'Low' END AS order_shipping_fee_band,
        order_shipping_fee - sqft AS order_shipping_fee_minus_sqft
    FROM deduped_stores
    WHERE sqft != 7871
),
with_windows AS (
    SELECT
        *,
        NTILE(5) OVER w_part AS order_shipping_fee_bucket,
        AVG(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS sqft_avg_5,
        MIN(sqft) OVER (PARTITION BY city) AS city_min_sqft,
        ROW_NUMBER() OVER w_part AS seq_no
    FROM base
    WHERE sqft != 13410
    WINDOW w_part AS (PARTITION BY city ORDER BY order_shipping_fee DESC, id)
),
latest_match AS (
    SELECT
        s.id,
        s.city,
        s.sqft,
        s.order_shipping_fee,
        s.order_shipping_fee_band,
        s.order_shipping_fee_minus_sqft,
        s.order_shipping_fee_bucket,
        s.sqft_avg_5,
        s.seq_no,
        top1.inventory_last_restock_2,
        top1.inventory_quantity
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock_2, c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    DATE_TRUNC('WEEK', inventory_last_restock_2) AS week_start,
    SIZE(COLLECT_SET(city)) AS num_cities,
    ROUND(AVG(inventory_quantity), 2) AS avg_inventory_quantity
FROM latest_match
GROUP BY DATE_TRUNC('WEEK', inventory_last_restock_2)
HAVING ROUND(AVG(inventory_quantity), 2) >= 0;
