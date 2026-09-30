WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        DATE_TRUNC('MONTH', shipped_date) AS shipped_date_period
    FROM shipments
    WHERE delivered_date > DATE '2023-09-12'
      AND order_id IS NOT NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        shipped_date_period,
        CASE WHEN cost >= 73 THEN 'High' WHEN cost >= 27 THEN 'Medium' ELSE 'Low' END AS cost_tier,
        LOWER(status) AS status_lower,
        COALESCE(carrier, 'OTHER') AS carrier_filled
    FROM filtered
    WHERE order_id IS NOT NULL
      AND order_id IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        SUM(cost) OVER w_hist AS running_cost,
        NTILE(3) OVER (PARTITION BY MONTH(shipped_date_period) ORDER BY cost DESC, id) AS cost_bucket
    FROM cleaned
    WHERE shipped_date_period >= '2025-03-04'
    WINDOW w_hist AS (PARTITION BY carrier ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned_shipments AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(carrier) AS carrier_lower,
        COALESCE(carrier, 'N/A') AS carrier_filled
    FROM with_windows
    WHERE shipped_date > DATE '2023-09-22'
      AND order_id IS NOT NULL
),
joined AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.carrier,
        s.shipped_date,
        s.cost_bucket,
        s.carrier_lower,
        p.discount,
        p.order_date
    FROM cleaned_shipments s
    LEFT JOIN orders p
        ON s.order_id = p.id
)
-- Aggregate with conditional metrics
SELECT
    order_id,
    COUNT_IF(carrier = 'DHL') AS dhl_events,
    COUNT_IF(carrier = 'FedEx') AS fedex_events,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    ROUND(SUM(discount) / NULLIF(COUNT(CASE WHEN carrier = 'UPS' THEN 1 END), 0), 2) AS discount_per_ups
FROM joined
GROUP BY order_id
ORDER BY dhl_events
