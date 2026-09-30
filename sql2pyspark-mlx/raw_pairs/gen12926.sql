WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(cost, 1) AS cost_rounded
    FROM shipments
    WHERE carrier IS NOT NULL
      AND cost <= 37
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date DESC, id) = 1
),
base_shipments AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        DATEDIFF(delivered_date, shipped_date) AS days_shipped_date_to_delivered_date,
        COALESCE(carrier, 'OTHER') AS carrier_filled
    FROM latest
    WHERE carrier IS NOT NULL
      AND carrier IS NOT DISTINCT FROM 'UPS'
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.delivered_date,
        src.days_shipped_date_to_delivered_date,
        src.carrier_filled,
        b.channel
    FROM base_shipments src
    LEFT JOIN orders b
        ON src.order_id = b.id
)
SELECT
    id,
    order_id,
    delivered_date,
    carrier_filled,
    channel,
    CASE WHEN channel RLIKE '^(web|store)$' THEN 'GROUP_A' WHEN channel RLIKE '^(mobile|partner)$' THEN 'GROUP_B' ELSE 'CORE' END AS channel_zone
FROM joined
ORDER BY id;
