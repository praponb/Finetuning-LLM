WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        WEEKOFYEAR(paid_at) paid_at_weekofyear,
        COALESCE(method, 'OTHER') resolved_method
    FROM payments
    WHERE paid_at > TO_TIMESTAMP('2023-10-30 04:00:00')
),
enriched AS (
    -- Attach orders attributes
    SELECT
        s.*,
        d.order_date,
        d.status AS order_status
    FROM base s
    INNER JOIN orders d
        ON s.order_id <=> d.id
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.paid_at,
        src.status,
        src.paid_at_weekofyear,
        src.order_date,
        o.order_date AS order_order_date
    FROM enriched src
    LEFT JOIN orders o
        ON src.order_id = o.id
),
enriched_payments AS (
    -- Attach orders attributes
    SELECT src.*, d.status AS order_status
    FROM with_dims src
    LEFT JOIN orders d
        ON src.order_id = d.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(paid_at_weekofyear) OVER w_running order_id_min_paid_at_weekofyear,
        AVG(paid_at_weekofyear) OVER w_hist paid_at_weekofyear_avg_3,
        SUM(paid_at_weekofyear) OVER w_running order_id_sum_paid_at_weekofyear,
        LEAD(paid_at_weekofyear, 2) OVER (PARTITION BY status ORDER BY order_date, id) next_paid_at_weekofyear,
        CUME_DIST() OVER (PARTITION BY MONTH(order_date), status ORDER BY paid_at_weekofyear DESC NULLS LAST) paid_at_weekofyear_cume_dist
    FROM enriched_payments
    WINDOW w_running AS (PARTITION BY order_id), w_hist AS (PARTITION BY MONTH(order_order_date), order_status ORDER BY paid_at_weekofyear, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
SELECT
    s.id,
    s.order_id,
    s.paid_at,
    s.status,
    s.paid_at_weekofyear,
    s.order_date,
    s.order_status,
    s.paid_at_weekofyear_avg_3,
    s.order_id_sum_paid_at_weekofyear,
    (SELECT MAX(x.unit_price) FROM order_items x WHERE x.order_id = s.order_id) AS max_order_item_unit_price
FROM scored s
ORDER BY id NULLS FIRST;
