WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(CURRENT_DATE(), shipped_date) AS days_since_shipped_date
    FROM shipments
    WHERE shipped_date >= DATE '2023-05-29'
      AND delivered_date <= '2025-07-01'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date DESC, id) = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY status ORDER BY cost DESC, id) AS cost_bucket,
        SUM(days_since_shipped_date) OVER (PARTITION BY order_id ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_days_since_shipped_date
    FROM deduped
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        days_since_shipped_date,
        cost_bucket
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status ORDER BY shipped_date) AS rn
        FROM enriched
    ) t
    WHERE rn = 1
)
SELECT
    s.id,
    s.order_id,
    s.carrier,
    s.delivered_date,
    s.days_since_shipped_date,
    s.cost_bucket,
    CASE WHEN carrier RLIKE '^(FedEx|Kerry)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS carrier_zone,
    (SELECT MAX(p2.amount) FROM payments p2 WHERE p2.order_id = s.order_id) AS max_payment_amount
FROM ranked s
ORDER BY max_payment_amount DESC NULLS FIRST, id
LIMIT 5;
