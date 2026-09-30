WITH valid AS (
    SELECT *, COALESCE(amount, 0) AS amount_filled
    FROM payments
    WHERE amount < 1270
),
joined AS (
    SELECT
        e.*,
        d.status AS order_status,
        d.order_date,
        d.amount AS order_amount
    FROM valid e
    LEFT JOIN orders d
        ON e.order_id = d.id
),
metrics AS (
    SELECT
        *,
        MIN(amount_filled) OVER w_grp AS order_status_min_amount_filled,
        COUNT(amount_filled) OVER (PARTITION BY method) AS method_count_amount_filled,
        MAX(amount) OVER (PARTITION BY status ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_max_7d
    FROM joined
    WINDOW w_grp AS (PARTITION BY order_status, order_id)
),
with_dims AS (
    SELECT src.*, b.order_date AS order_order_date
    FROM metrics src
    LEFT JOIN orders b
        ON src.order_id = b.id
),
scored AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        amount_filled,
        order_status,
        order_date,
        order_amount,
        order_status_min_amount_filled,
        method_count_amount_filled,
        amount_max_7d,
        order_order_date,
        LAG(amount, 1) OVER (PARTITION BY method ORDER BY method_count_amount_filled, id) AS prev_amount,
        AVG(order_status_min_amount_filled) OVER (PARTITION BY order_status ORDER BY amount_max_7d, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS order_status_min_amount_filled_avg_6,
        LAST_VALUE(amount_max_7d) IGNORE NULLS OVER (PARTITION BY order_status, status ORDER BY amount_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount_max_7d,
        SUM(order_amount) OVER (PARTITION BY method, order_id ORDER BY order_order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_order_amount
    FROM with_dims
),
with_dims_payments AS (
    SELECT
        e.*,
        o.shipping_fee,
        o.status AS order_status_2
    FROM scored e
    INNER JOIN orders o
        ON e.order_id = o.id
)
SELECT
    id,
    amount,
    paid_at,
    status,
    amount_filled,
    order_date,
    order_amount,
    order_status_min_amount_filled,
    order_order_date
FROM with_dims_payments
