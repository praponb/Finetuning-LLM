WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        cost - price AS margin,
        COALESCE(brand, 'OTHER') AS brand_clean
    FROM products
    WHERE MONTH(launch_date) IN (1, 5, 10)
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.price,
        s.cost,
        s.launch_date,
        s.tags,
        s.supplier_id,
        s.brand_clean,
        top1.order_item_unit_price
    FROM filtered s
    JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY order_item_unit_price, id) AS event_seq,
        NTILE(4) OVER (PARTITION BY supplier_id ORDER BY order_item_unit_price, id) AS order_item_unit_price_bucket,
        LEAD(cost, 1) OVER (PARTITION BY YEAR(launch_date), supplier_id ORDER BY cost, id) AS next_cost
    FROM with_latest
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (ORDER BY price DESC, id) AS price_bucket,
        LAG(next_cost, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', launch_date), supplier_id ORDER BY cost, id) AS prev_next_cost,
        ROUND(order_item_unit_price_bucket / SUM(order_item_unit_price_bucket) OVER (PARTITION BY supplier_id), 4) AS order_item_unit_price_bucket_share,
        ROUND(cost / SUM(cost) OVER (PARTITION BY supplier_id), 4) AS cost_share
    FROM scored
    WHERE order_item_unit_price <= 260
)
SELECT
    id,
    product_name,
    price,
    cost,
    launch_date,
    brand_clean,
    event_seq,
    price_bucket,
    prev_next_cost
FROM with_windows
ORDER BY prev_next_cost DESC;
