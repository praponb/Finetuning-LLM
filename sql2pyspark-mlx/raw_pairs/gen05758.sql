WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        CASE WHEN cost >= 95 THEN 'High' WHEN cost >= 14 THEN 'Medium' ELSE 'Low' END AS cost_level
    FROM shipments
    WHERE delivered_date >= DATE '2023-04-28'
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.shipped_date,
        src.cost_level,
        b.discount,
        b.order_date,
        b.amount
    FROM valid src
    LEFT JOIN orders b
        ON src.order_id <=> b.id
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount NULLS LAST, id) AS row_num
    FROM joined
    QUALIFY row_num = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.shipped_date,
        a.cost_level,
        a.amount,
        a.row_num,
        d.status,
        d.order_date AS order_order_date
    FROM deduped a
    INNER JOIN orders d
        ON a.order_id = d.id
)
SELECT
    s.id,
    s.order_id,
    s.shipped_date,
    s.cost_level,
    s.amount,
    s.status,
    s.order_order_date,
    CASE WHEN status RLIKE '^(CANCELLED|COMPLETED)$' THEN 'GROUP_B' WHEN status RLIKE '^(PENDING|SHIPPED)$' THEN 'CORE' ELSE 'GROUP_A' END AS status_zone,
    (SELECT MIN(s2.cost) FROM shipments s2 WHERE s2.order_id = s.order_id) AS min_shipment_cost
FROM enriched s
ORDER BY id DESC NULLS LAST, order_id;
