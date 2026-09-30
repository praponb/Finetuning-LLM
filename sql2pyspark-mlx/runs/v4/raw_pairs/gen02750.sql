WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(status)) AS status_title
    FROM orders
    WHERE customer_id IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY store_id ORDER BY discount DESC, id) AS discount_bucket,
        FIRST_VALUE(shipping_fee) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_shipping_fee,
        DENSE_RANK() OVER (ORDER BY amount DESC) AS amount_dense_rank
    FROM cleaned
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments ch
            WHERE ch.order_id = s.id
              AND ch.paid_at >= s.order_date
        )
),
matched_keys AS (
    -- Set difference on keys
    SELECT customer_id, id
    FROM not_flagged
    EXCEPT
    SELECT customer_id, id
    FROM orders
    WHERE store_id IS NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM not_flagged s
    JOIN matched_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT store_id, channel
    FROM verified
    EXCEPT
    SELECT store_id, channel
    FROM orders
    WHERE status IN ('SHIPPED', 'CANCELLED')
),
verified_orders AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    JOIN kept_keys k ON s.store_id = k.store_id AND s.channel = k.channel
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            status,
            customer_id,
            channel,
            discount
        FROM verified_orders
    ) src
    PIVOT (
        SUM(discount)
        FOR channel IN ('store' AS store, 'web' AS web, 'partner' AS partner)
    )
)
SELECT
    s.status,
    s.customer_id,
    s.store,
    s.web,
    (SELECT MAX(x.discount) FROM orders x WHERE x.customer_id = s.customer_id) AS max_order_discount
FROM pivoted s
ORDER BY store DESC NULLS LAST, status DESC;
