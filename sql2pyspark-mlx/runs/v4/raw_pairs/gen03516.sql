WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        COUNT(shipping_fee) OVER (PARTITION BY status, customer_id) AS status_count_shipping_fee,
        NTILE(10) OVER (PARTITION BY channel ORDER BY discount DESC, id) AS discount_bucket,
        CORR(amount, shipping_fee) OVER w_ordered AS corr_amount_shipping_fee
    FROM orders
    WINDOW w_ordered AS (PARTITION BY customer_id, channel)
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.quantity
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched_orders AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(corr_amount_shipping_fee) OVER (PARTITION BY store_id ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_corr_amount_shipping_fee,
        LAST_VALUE(status_count_shipping_fee, TRUE) OVER (PARTITION BY channel ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_status_count_shipping_fee
    FROM latest_match
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('WEEK', order_date) AS week_start,
        DATE_TRUNC('YEAR', order_date) AS year_start,
        COUNT(DISTINCT store_id) AS distinct_store_ids,
        ROUND(SUM(corr_amount_shipping_fee) / NULLIF(COUNT(CASE WHEN status = 'RETURNED' THEN 1 END), 0), 2) AS corr_amount_shipping_fee_per_returned,
        SIZE(COLLECT_SET(status)) AS num_status,
        GROUPING_ID() AS grouping_level
    FROM enriched_orders
    GROUP BY CUBE (DATE_TRUNC('WEEK', order_date), DATE_TRUNC('YEAR', order_date))
    HAVING num_status < 1
)
SELECT
    week_start,
    year_start,
    num_status
FROM summary;
