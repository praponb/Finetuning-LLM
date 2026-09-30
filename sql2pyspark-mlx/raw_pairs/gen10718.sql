WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        UPPER(status) AS status_upper,
        COALESCE(carrier, 'N/A') AS resolved_carrier
    FROM shipments
    WHERE carrier IN ('UPS', 'DHL')
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date NULLS FIRST, id) AS rn
    FROM base
    QUALIFY rn = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost DESC, id) AS rn_2
    FROM ranked
    QUALIFY rn_2 = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY status, carrier ORDER BY cost) AS cost_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date, id) AS event_seq
    FROM latest
),
enriched AS (
    -- Attach orders attributes
    SELECT
        s.*,
        p.channel,
        p.amount
    FROM metrics s
    LEFT JOIN orders p
        ON s.order_id = p.id
    WHERE p.channel IN ('partner', 'mobile')
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        carrier,
        channel,
        event_seq
    FROM enriched
) src
PIVOT (
    AVG(event_seq)
    FOR channel IN ('mobile' AS mobile, 'partner' AS partner)
)
