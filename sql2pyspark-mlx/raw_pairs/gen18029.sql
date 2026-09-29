WITH deduped AS (
    SELECT
        id,
        category,
        brand,
        cost,
        supplier_id
    FROM products
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY launch_date NULLS FIRST, id) = 1
),
metrics AS (
    SELECT
        *,
        LEAD(cost) OVER w_part AS next_cost,
        LAG(cost) OVER w_part AS prev_cost,
        ROW_NUMBER() OVER w_part AS event_seq
    FROM deduped
    WINDOW w_part AS (PARTITION BY brand ORDER BY cost, id)
),
with_latest AS (
    SELECT
        s.*,
        top1.store_sale_revenue,
        top1.store_sale_sale_date
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.revenue AS store_sale_revenue, c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match AS (
    SELECT
        s.id,
        s.category,
        s.brand,
        s.cost,
        s.next_cost,
        s.event_seq,
        s.store_sale_revenue,
        top1.quantity
    FROM with_latest s
    JOIN LATERAL (
            SELECT c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    category,
    next_cost,
    event_seq,
    store_sale_revenue,
    quantity
FROM latest_match
ORDER BY quantity DESC
