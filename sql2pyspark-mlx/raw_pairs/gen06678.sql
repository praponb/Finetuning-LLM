WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sqft,
        AVG(sqft) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) sqft_avg_6,
        SUM(sqft) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_sqft
    FROM stores
    WHERE city = 'London'
),
with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.city,
        s.opened_date,
        s.running_sqft,
        s.cumulative_sqft,
        top1.order_discount
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.discount AS order_discount
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest_stores AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.city,
        s.running_sqft,
        s.cumulative_sqft,
        s.order_discount,
        x.store_sale_sale_date,
        x.units
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date, c.units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_latest_stores s
    WHERE EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.order_date < s.store_sale_sale_date
        )
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.revenue
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        store_name,
        city,
        order_discount,
        revenue
    FROM latest_match
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY units DESC NULLS LAST, id) = 1
)
-- Running and moving aggregates
SELECT
    id,
    store_name,
    order_discount,
    NTILE(5) OVER w_recent order_discount_bucket,
    ROW_NUMBER() OVER w_ordered event_seq
FROM deduped
WINDOW w_recent AS (PARTITION BY city ORDER BY order_discount DESC, id), w_ordered AS (PARTITION BY city ORDER BY revenue DESC, id);
