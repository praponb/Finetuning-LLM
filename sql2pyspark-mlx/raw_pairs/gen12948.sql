WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.quantity = 4
        )
),
remaining_keys AS (
    -- Set difference on keys
    SELECT customer_id, status
    FROM eligible
    EXCEPT DISTINCT
    SELECT customer_id, status
    FROM orders
    WHERE customer_id IN (17, 25, 33, 34)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.status = k.status
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments x
            WHERE x.order_id = s.id
              AND MONTH(x.delivered_date) IN (2, 8, 10)
        )
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CAST(discount AS INT) AS discount_int
    FROM kept
    WHERE channel IS DISTINCT FROM 'store'
      AND order_date >= DATE '2023-04-11'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        customer_id,
        store_id,
        order_date,
        amount,
        shipping_fee,
        discount_int,
        CORR(shipping_fee, discount) OVER (PARTITION BY MONTH(order_date), status) AS corr_shipping_fee_discount,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY discount ASC NULLS FIRST) AS discount_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY YEAR(order_date), status ORDER BY amount DESC) AS amount_percent_rank
    FROM cleaned
    WHERE discount < 0.3
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ABS(corr_shipping_fee_discount) AS abs_corr_shipping_fee_discount,
        CAST(shipping_fee AS INT) AS shipping_fee_int
    FROM with_windows
    WHERE order_date >= TO_DATE('2023-08-19')
      AND customer_id IN (9, 21, 22, 33)
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.customer_id,
        src.store_id,
        src.order_date,
        src.discount_int,
        src.amount_percent_rank,
        src.abs_corr_shipping_fee_discount,
        c.segment,
        c.credit_limit
    FROM base src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
stats AS (
    -- Grouped metrics
    SELECT
        customer_id,
        APPROX_PERCENTILE(discount_int, 0.5) AS discount_int_p50,
        SUM(CASE WHEN segment = 'SMB' THEN credit_limit ELSE 0 END) AS smb_credit_limit,
        SUM(CASE WHEN segment = 'Consumer' THEN amount_percent_rank ELSE 0 END) AS consumer_amount_percent_rank,
        COUNT_IF(segment = 'SMB') AS smb_events,
        SUM(CASE WHEN segment = 'Corporate' THEN discount_int ELSE 0 END) AS corporate_discount_int
    FROM enriched
    GROUP BY customer_id
)
SELECT
    s.customer_id,
    s.discount_int_p50,
    s.smb_events,
    s.corporate_discount_int,
    (SELECT MAX(o2.amount) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS max_order_amount
FROM stats s
ORDER BY smb_events ASC;
