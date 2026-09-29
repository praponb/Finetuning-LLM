WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        paid_at,
        status,
        LOWER(method) AS method_lower,
        ROUND(amount * 1.1, 2) AS amount_scaled
    FROM payments
    WHERE method IS NOT DISTINCT FROM 'paypal'
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, status
    FROM filtered
    EXCEPT DISTINCT
    SELECT id, status
    FROM payments
    WHERE amount <= 1560
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.id = k.id AND s.status = k.status
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(amount_scaled) OVER w_ordered AS status_count_amount_scaled,
        CUME_DIST() OVER (PARTITION BY TO_DATE(paid_at) ORDER BY amount_scaled NULLS FIRST) AS amount_scaled_cume_dist,
        AVG(amount_scaled) OVER (PARTITION BY order_id ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_scaled_avg_7d
    FROM verified
    WHERE TO_DATE(paid_at) >= '2025-01-01'
    WINDOW w_ordered AS (PARTITION BY status, order_id)
),
scored_payments AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(amount_scaled) OVER (PARTITION BY DATE_TRUNC('WEEK', paid_at), order_id) AS paid_at_period_max_amount_scaled,
        CUME_DIST() OVER (PARTITION BY status, order_id ORDER BY status_count_amount_scaled DESC) AS status_count_amount_scaled_cume_dist
    FROM scored
),
enriched AS (
    -- Attach orders attributes
    SELECT
        a.order_id,
        a.paid_at,
        a.method_lower,
        a.amount_scaled_avg_7d,
        b.amount,
        b.order_date
    FROM scored_payments a
    INNER JOIN orders b
        ON a.order_id = b.id
),
report AS (
    SELECT
        s.order_id,
        s.method_lower,
        s.amount_scaled_avg_7d,
        s.amount,
        s.order_date,
        (SELECT AVG(o2.unit_price) FROM order_items o2 WHERE o2.order_id = s.order_id) AS avg_order_item_unit_price
    FROM enriched s
)
SELECT
    amount_scaled_avg_7d,
    amount,
    order_date,
    avg_order_item_unit_price
FROM report;
