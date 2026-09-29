WITH with_dims AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.carrier,
        e.cost,
        e.status,
        p.status AS order_status,
        p.channel,
        p.shipping_fee
    FROM shipments e
    LEFT JOIN orders p
        ON e.order_id = p.id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        carrier,
        status,
        order_status,
        channel,
        shipping_fee,
        CONCAT(status, ' ', channel) AS status_channel
    FROM with_dims
    WHERE carrier IS NOT NULL
      AND carrier IN ('DHL', 'Kerry')
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, order_status ORDER BY shipping_fee DESC, id) AS latest_rank
    FROM filtered
    QUALIFY latest_rank = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(shipping_fee) OVER (PARTITION BY status, carrier ORDER BY shipping_fee, id) AS prev_shipping_fee,
        ROW_NUMBER() OVER (PARTITION BY status, carrier ORDER BY shipping_fee, id) AS seq_no,
        LAST_VALUE(order_status, TRUE) OVER (PARTITION BY carrier ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_order_status
    FROM ranked
    WHERE order_status NOT IN ('SHIPPED', 'CANCELLED', 'RETURNED')
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        carrier,
        status,
        order_status,
        channel,
        status_channel,
        latest_rank,
        seq_no,
        last_known_order_status,
        INITCAP(TRIM(carrier)) AS carrier_title,
        seq_no + prev_shipping_fee AS seq_no_plus_prev_shipping_fee,
        COALESCE(order_status, 'N/A') AS order_status_clean
    FROM with_windows
    WHERE prev_shipping_fee IS NULL
),
valid_shipments AS (
    -- Keep recent, valid rows
    SELECT
        *,
        seq_no_plus_prev_shipping_fee * latest_rank AS seq_no_plus_prev_shipping_fee_times_latest_rank,
        COALESCE(carrier, 'NONE') AS carrier_filled
    FROM valid
    WHERE order_status IS NOT DISTINCT FROM 'SHIPPED'
      AND order_status = 'SHIPPED'
)
-- Grouped metrics
SELECT
    CASE WHEN seq_no_plus_prev_shipping_fee_times_latest_rank >= 74 THEN 'HIGH' ELSE 'LOW' END AS seq_no_plus_prev_shipping_fee_times_latest_rank_tier,
    status,
    COUNT(CASE WHEN channel = 'web' THEN 1 END) AS web_count,
    SUM(CASE WHEN carrier = 'Kerry' THEN seq_no_plus_prev_shipping_fee ELSE 0 END) AS kerry_seq_no_plus_prev_shipping_fee,
    ROUND(SUM(seq_no_plus_prev_shipping_fee) / NULLIF(COUNT(CASE WHEN channel = 'partner' THEN 1 END), 0), 2) AS seq_no_plus_prev_shipping_fee_per_partner,
    COUNT_IF(carrier = 'UPS') AS ups_events,
    ROUND(AVG(seq_no_plus_prev_shipping_fee), 2) AS avg_seq_no_plus_prev_shipping_fee,
    AVG(seq_no_plus_prev_shipping_fee_times_latest_rank) FILTER (WHERE carrier = 'UPS') AS ups_avg_seq_no_plus_prev_shipping_fee_times_latest_rank
FROM valid_shipments
GROUP BY CASE WHEN seq_no_plus_prev_shipping_fee_times_latest_rank >= 74 THEN 'HIGH' ELSE 'LOW' END, status
