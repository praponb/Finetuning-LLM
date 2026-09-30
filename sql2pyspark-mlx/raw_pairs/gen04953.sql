WITH kept_keys AS (
    -- Set difference on keys
    SELECT status
    FROM orders
    EXCEPT ALL
    SELECT status
    FROM orders
    WHERE shipping_fee > 14
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM orders s
    INNER JOIN kept_keys k ON s.status = k.status
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at < s.order_date
        )
),
enriched AS (
    -- Attach stores attributes
    SELECT
        s.customer_id,
        s.store_id,
        s.order_date,
        s.status,
        s.amount,
        s.discount,
        p.city,
        p.sqft
    FROM kept s
    INNER JOIN stores p
        ON s.store_id = p.id
),
enriched_orders AS (
    -- Attach stores attributes
    SELECT
        e.*,
        p.region,
        p.store_name
    FROM enriched e
    LEFT JOIN stores p
        ON e.store_id = p.id
),
scored AS (
    -- Running and moving aggregates
    SELECT *, CUME_DIST() OVER (PARTITION BY YEAR(order_date), city ORDER BY discount DESC) AS discount_cume_dist
    FROM enriched_orders
    WHERE city IN ('London', 'Tokyo')
)
SELECT
    s.customer_id,
    s.store_id,
    s.order_date,
    s.discount,
    s.city,
    s.sqft,
    s.region,
    s.discount_cume_dist,
    (SELECT AVG(x.discount) FROM orders x WHERE x.customer_id = s.customer_id) AS avg_order_discount
FROM scored s
ORDER BY region DESC NULLS LAST
