WITH enriched AS (
    -- Attach orders attributes
    SELECT s.*, b.shipping_fee
    FROM shipments s
    LEFT JOIN orders b
        ON s.order_id = b.id
    WHERE b.status IN ('RETURNED', 'SHIPPED', 'CANCELLED')
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipping_fee NULLS LAST, id) = 1
),
enriched_shipments AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.carrier,
        src.delivered_date,
        b.amount
    FROM ranked src
    LEFT JOIN orders b
        ON src.order_id = b.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_FORMAT(delivered_date, 'yyyy-MM') AS delivered_date_label
    FROM enriched_shipments
    WHERE carrier IS NOT NULL
      AND carrier IN ('Flash', 'FedEx', 'Kerry')
)
-- Running and moving aggregates
SELECT
    id,
    carrier,
    delivered_date,
    amount,
    delivered_date_label,
    NTILE(5) OVER (PARTITION BY order_id, carrier ORDER BY amount DESC, id) AS amount_bucket,
    LEAD(amount, 2) OVER w_running AS next_amount
FROM base
WINDOW w_running AS (PARTITION BY carrier, order_id ORDER BY delivered_date, id)
