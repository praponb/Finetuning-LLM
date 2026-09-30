WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY order_id, carrier ORDER BY cost DESC) AS row_num
    FROM shipments
    QUALIFY row_num = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.carrier,
        s.shipped_date,
        s.status,
        s.row_num,
        o.order_date,
        o.shipping_fee
    FROM ranked s
    LEFT JOIN orders o
        ON s.order_id = o.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        order_date,
        AVG(shipping_fee) OVER w_running AS shipping_fee_avg_5_prior,
        LEAD(shipping_fee, 2, 0) OVER (ORDER BY shipping_fee, id) AS next_shipping_fee
    FROM enriched
    WHERE shipped_date <= '2024-10-01'
    WINDOW w_running AS (PARTITION BY carrier ORDER BY row_num, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.shipped_date,
        b.discount,
        b.amount
    FROM metrics e
    LEFT JOIN orders b
        ON e.order_id = b.id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT order_id
    FROM joined
    EXCEPT DISTINCT
    SELECT order_id
    FROM shipments
    WHERE delivered_date BETWEEN '2024-12-13' AND '2025-05-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    JOIN remaining_keys k ON s.order_id = k.order_id
),
grouped AS (
    -- Grouped metrics
    SELECT
        order_id,
        APPROX_PERCENTILE(amount, 0.9) AS amount_p90,
        APPROX_PERCENTILE(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
        PERCENTILE_APPROX(discount, ARRAY(0.25, 0.5, 0.75)) AS discount_quartiles,
        PERCENTILE_APPROX(discount, 0.95) AS discount_p95,
        ROUND(AVG(amount), 2) AS avg_amount,
        GROUPING(order_id) AS grouping_id
    FROM verified
    GROUP BY ROLLUP (order_id)
)
SELECT
    order_id,
    discount_quartiles,
    avg_amount,
    grouping_id
FROM grouped;
