WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, QUARTER(order_date) AS order_date_quarter
    FROM orders
    WHERE order_date >= DATE '2023-09-17'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.order_id IN (15, 16, 23, 29)
        )
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        MIN(discount) OVER w_grp AS running_min_discount,
        AVG(discount) OVER w_grp AS running_avg_discount,
        MAX(discount) OVER w_grp AS running_max_discount
    FROM kept
    WHERE order_date < '2023-07-01'
    WINDOW w_grp AS (PARTITION BY channel, status ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.customer_id,
        e.store_id,
        e.channel,
        e.running_min_discount,
        e.running_avg_discount,
        p.sqft
    FROM scored e
    LEFT JOIN stores p
        ON e.store_id = p.id
),
with_last AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.running_avg_discount,
        s.sqft,
        latest.carrier
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.carrier
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.delivered_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Grouped metrics
SELECT
    CASE WHEN running_avg_discount >= 0.36 THEN 'LARGE' ELSE 'SMALL' END AS running_avg_discount_tier,
    carrier,
    SIZE(COLLECT_SET(carrier)) AS num_carriers,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_last
GROUP BY GROUPING SETS ((CASE WHEN running_avg_discount >= 0.36 THEN 'LARGE' ELSE 'SMALL' END, carrier), (CASE WHEN running_avg_discount >= 0.36 THEN 'LARGE' ELSE 'SMALL' END), ())
HAVING SIZE(COLLECT_SET(carrier)) > 1
ORDER BY running_avg_discount_tier DESC;
