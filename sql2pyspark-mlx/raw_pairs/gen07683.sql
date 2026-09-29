WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        status,
        CASE WHEN status IN ('LOST', 'RETURNED') THEN 'Group A' ELSE 'Other' END AS status_group,
        DATEDIFF(CURRENT_DATE(), shipped_date) AS days_since_shipped_date
    FROM shipments
    WHERE delivered_date >= DATE '2023-06-15'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT status
    FROM filtered
    INTERSECT ALL
    SELECT status
    FROM shipments
    WHERE order_id IS NOT NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.status = k.status
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.order_id,
        e.carrier,
        e.shipped_date,
        e.status,
        e.status_group,
        p.discount
    FROM kept_rows e
    LEFT JOIN orders p
        ON e.order_id = p.id
),
enriched AS (
    -- Attach orders attributes
    SELECT
        a.order_id,
        a.carrier,
        a.status,
        a.status_group,
        d.discount AS order_discount
    FROM joined a
    LEFT JOIN orders d
        ON a.order_id = d.id
    WHERE d.channel IS NOT NULL
),
joined_shipments AS (
    -- Attach orders attributes
    SELECT
        e.order_id,
        e.carrier,
        e.status,
        e.order_discount,
        p.discount,
        p.status AS order_status
    FROM enriched e
    LEFT JOIN orders p
        ON e.order_id = p.id
)
SELECT
    carrier,
    order_discount,
    discount,
    order_status
FROM joined_shipments
ORDER BY discount DESC NULLS LAST;
