WITH with_dims AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.carrier,
        src.status,
        p.status AS order_status,
        p.channel,
        p.discount
    FROM shipments src
    INNER JOIN orders p
        ON src.order_id = p.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        carrier,
        order_status,
        channel
    FROM with_dims
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY discount, id) = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(channel) AS channel_lower,
        LOWER(channel) AS channel_lower_2
    FROM deduped
    WHERE carrier IS DISTINCT FROM 'UPS'
      AND order_status IN ('SHIPPED', 'RETURNED')
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN channel IN ('mobile', 'partner') THEN 'Group A' ELSE 'Other' END AS channel_group
    FROM base
    WHERE order_status IN ('CANCELLED', 'SHIPPED')
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        carrier,
        order_status,
        channel,
        channel_lower,
        channel_lower_2,
        channel_group,
        CASE WHEN channel IN ('web', 'mobile') THEN 'Tier 1' ELSE 'Other' END AS channel_group_2,
        COALESCE(channel, 'UNKNOWN') AS channel_filled
    FROM valid
    WHERE order_status IS NOT NULL
)
SELECT
    id,
    carrier,
    channel_lower,
    channel_lower_2,
    channel_group,
    channel_group_2,
    channel_filled
FROM filtered
ORDER BY id NULLS LAST, carrier;
