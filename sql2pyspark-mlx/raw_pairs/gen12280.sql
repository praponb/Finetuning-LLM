WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(carrier)) carrier_title,
        COALESCE(carrier, 'DIRECT') carrier_filled
    FROM shipments
    WHERE shipped_date >= DATE '2023-05-31'
      AND carrier IS NOT DISTINCT FROM 'UPS'
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.*,
        d.order_date,
        d.status AS order_status,
        d.discount
    FROM base e
    LEFT JOIN orders d
        ON e.order_id = d.id
    WHERE d.store_id IS NOT NULL
),
enriched_shipments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        carrier,
        cost,
        carrier_filled,
        order_date,
        discount,
        LAST_VALUE(cost, TRUE) OVER (PARTITION BY order_status ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_cost,
        NTILE(4) OVER (ORDER BY cost DESC, id) cost_bucket,
        LEAD(cost, 1) OVER (PARTITION BY status, order_id ORDER BY shipped_date, id) next_cost
    FROM enriched
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        carrier,
        cost,
        carrier_filled,
        order_date,
        discount,
        last_known_cost,
        cost_bucket,
        next_cost / NULLIF(discount, 0) next_cost_per_discount
    FROM enriched_shipments
    WHERE carrier IS DISTINCT FROM 'Flash'
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    carrier,
    metric_name,
    metric_value
FROM (
    SELECT
        id,
        carrier,
        next_cost_per_discount,
        cost,
        discount,
        last_known_cost
    FROM filtered
) src
UNPIVOT (
    metric_value FOR metric_name IN (next_cost_per_discount, cost, discount, last_known_cost)
) unp
