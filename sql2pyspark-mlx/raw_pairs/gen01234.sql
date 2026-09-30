WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        status,
        DATEDIFF(CURRENT_DATE(), shipped_date) / 7 AS weeks_since_shipped_date
    FROM shipments
    WHERE shipped_date > TO_DATE('2023-03-29')
      AND order_id IS NOT NULL
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        status,
        CAST(weeks_since_shipped_date AS INT) AS weeks_since_shipped_date_int,
        COALESCE(carrier, 'OTHER') AS resolved_carrier
    FROM filtered
    WHERE delivered_date >= DATE '2023-03-08'
),
enriched AS (
    -- Attach orders attributes
    SELECT e.*, d.discount
    FROM valid e
    LEFT JOIN orders d
        ON e.order_id = d.id
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.carrier,
        a.cost,
        a.status,
        a.weeks_since_shipped_date_int,
        a.resolved_carrier,
        a.discount,
        b.channel,
        b.discount AS order_discount
    FROM enriched a
    LEFT JOIN orders b
        ON a.order_id = b.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(discount AS INT) AS discount_int,
        CASE WHEN cost >= 98 THEN 'High' WHEN cost >= 23 THEN 'Medium' ELSE 'Low' END AS cost_tier
    FROM with_dims
    WHERE cost BETWEEN 35 AND 52
),
base_shipments AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        carrier,
        cost,
        status,
        weeks_since_shipped_date_int,
        resolved_carrier,
        order_discount,
        discount_int,
        INITCAP(TRIM(carrier)) AS carrier_title,
        SUBSTRING(status, 1, 4) AS status_prefix
    FROM base
    WHERE status IN ('RETURNED', 'IN_TRANSIT', 'LOST')
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        order_id,
        order_discount,
        cost
    FROM base_shipments
) src
UNPIVOT INCLUDE NULLS (
    val FOR kpi IN (order_discount AS ORDER_DISCOUNT, cost AS COST_VAL)
) long_rows
