WITH remaining_keys AS (
    -- Set difference on keys
    SELECT id, carrier
    FROM shipments
    EXCEPT
    SELECT id, carrier
    FROM shipments
    WHERE delivered_date > '2024-12-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM shipments s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.carrier = k.carrier
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        cost,
        status,
        PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('week', delivered_date), status ORDER BY cost NULLS FIRST) AS cost_percent_rank,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY cost) AS cost_cume_dist
    FROM reconciled
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.*,
        o.order_date,
        o.amount
    FROM metrics e
    LEFT JOIN orders o
        ON e.order_id = o.id
    WHERE o.amount < 1250
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.shipped_date,
        e.cost,
        e.cost_percent_rank,
        e.order_date,
        b.amount AS order_amount
    FROM joined e
    LEFT JOIN orders b
        ON e.order_id <=> b.id
),
enriched_shipments AS (
    -- Attach orders attributes
    SELECT
        a.order_id,
        a.shipped_date,
        a.order_date,
        b.order_date AS order_order_date,
        b.status
    FROM enriched a
    LEFT JOIN orders b
        ON a.order_id = b.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', order_date) AS quarter_start,
    order_id,
    COUNT_IF(status = 'PENDING') AS pending_events,
    COUNT(CASE WHEN status = 'SHIPPED' THEN 1 END) AS shipped_count,
    SIZE(COLLECT_SET(status)) AS num_status
FROM enriched_shipments
GROUP BY quarter_start, order_id
HAVING COUNT_IF(status = 'PENDING') > 1;
