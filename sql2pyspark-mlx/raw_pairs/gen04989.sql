WITH joined AS (
    -- Attach orders attributes
    SELECT
        a.*,
        p.amount,
        p.discount,
        p.order_date
    FROM shipments a
    LEFT JOIN orders p
        ON a.order_id = p.id
    WHERE p.amount < 450
),
enriched AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.cost,
        a.status,
        a.amount,
        a.order_date,
        b.shipping_fee
    FROM joined a
    LEFT JOIN orders b
        ON a.order_id = b.id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_FORMAT(order_date, 'MMM yyyy') AS order_date_label,
        ROUND(shipping_fee, 1) AS shipping_fee_rounded
    FROM enriched
    WHERE status IN ('RETURNED', 'IN_TRANSIT', 'LOST')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        cost,
        status,
        order_date,
        shipping_fee_rounded,
        LAST_VALUE(amount) IGNORE NULLS OVER (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
        STDDEV_SAMP(cost) OVER (PARTITION BY status) AS status_stddev_samp_cost,
        LEAD(shipping_fee_rounded, 1, 0) OVER w_part AS next_shipping_fee_rounded
    FROM filtered
    WINDOW w_part AS (PARTITION BY status ORDER BY cost, id)
),
filtered_shipments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        cost,
        status,
        shipping_fee_rounded,
        last_known_amount,
        status_stddev_samp_cost,
        next_shipping_fee_rounded,
        cost - shipping_fee_rounded AS cost_minus_shipping_fee_rounded,
        COALESCE(next_shipping_fee_rounded, 0) AS next_shipping_fee_rounded_filled
    FROM metrics
    WHERE cost BETWEEN 41 AND 73
),
metrics_shipments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        cost,
        next_shipping_fee_rounded_filled,
        NTILE(4) OVER (ORDER BY shipping_fee_rounded DESC, id) AS shipping_fee_rounded_bucket,
        NTILE(3) OVER (PARTITION BY status ORDER BY last_known_amount DESC, id) AS last_known_amount_bucket
    FROM filtered_shipments
)
-- Running and moving aggregates
SELECT
    id,
    cost,
    next_shipping_fee_rounded_filled,
    shipping_fee_rounded_bucket,
    last_known_amount_bucket,
    AVG(cost) OVER (ORDER BY shipping_fee_rounded_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_cost,
    COUNT(cost) OVER (ORDER BY shipping_fee_rounded_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_cost,
    SUM(cost) OVER (ORDER BY shipping_fee_rounded_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_cost
FROM metrics_shipments
