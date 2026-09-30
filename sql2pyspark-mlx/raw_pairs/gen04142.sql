WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        t.id,
        t.customer_id,
        t.created_at,
        t.coupon_codes,
        pos,
        li_item.qty,
        li_item.price
    FROM carts t
    LATERAL VIEW OUTER POSEXPLODE(t.items) li AS pos, li_item
),
flattened_carts AS (
    -- Unnest the array into one row per element
    SELECT
        customer_id,
        pos,
        qty,
        EXPLODE_OUTER(coupon_codes) AS value
    FROM flattened
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT *, CUME_DIST() OVER w_hist AS qty_cume_dist
    FROM flattened_carts
    WINDOW w_hist AS (PARTITION BY value ORDER BY qty DESC NULLS LAST)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        customer_id,
        pos,
        qty,
        DENSE_RANK() OVER (ORDER BY qty DESC) AS qty_dense_rank
    FROM with_windows
    WHERE pos <> 3
),
scored AS (
    -- Running and moving aggregates
    SELECT
        customer_id,
        pos,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY qty ASC) AS qty_cume_dist,
        RANK() OVER (ORDER BY qty ASC) AS qty_rank
    FROM metrics
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.customer_id,
        src.pos,
        src.qty_cume_dist,
        src.qty_rank,
        d.name,
        d.city
    FROM scored src
    LEFT JOIN customers d
        ON src.customer_id = d.id
)
SELECT
    s.customer_id,
    s.qty_rank,
    s.city,
    (SELECT MAX(t2.discount) FROM orders t2 WHERE t2.customer_id = s.customer_id) AS max_order_discount
FROM with_dims s
ORDER BY city DESC
