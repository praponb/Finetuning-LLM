WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ADD_MONTHS(order_date, 12) AS order_date_plus_months,
        COALESCE(channel, 'unassigned') AS channel_filled,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM orders
    WHERE channel IS NOT NULL
      AND amount < 1190
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        order_date,
        status,
        amount,
        shipping_fee,
        channel_filled
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY discount DESC NULLS FIRST, id) = 1
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.carrier IS NULL
        )
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT customer_id, status
    FROM not_flagged
    INTERSECT
    SELECT customer_id, status
    FROM orders
    WHERE amount > 1560
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM not_flagged s
    JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.status = k.status
)
SELECT
    customer_id,
    shipping_fee,
    channel_filled
FROM kept_rows;
