WITH cleaned AS (
    SELECT
        *,
        SUBSTRING(status, 1, 3) AS status_prefix,
        COALESCE(carrier, 'N/A') AS carrier_filled
    FROM shipments
    WHERE carrier IS NOT NULL
      AND YEAR(delivered_date) = 2025
),
joined AS (
    SELECT
        a.id,
        a.order_id,
        a.carrier,
        a.cost,
        a.status,
        a.status_prefix,
        a.carrier_filled,
        b.shipping_fee,
        b.order_date
    FROM cleaned a
    LEFT JOIN orders b
        ON a.order_id = b.id
    WHERE b.amount > 770
),
filtered AS (
    SELECT *, CASE WHEN status = 'LOST' THEN 1 ELSE 0 END AS is_lost
    FROM joined
    WHERE order_id IS NOT NULL
),
valid AS (
    SELECT
        *,
        DATE_FORMAT(order_date, 'MMM yyyy') AS order_date_label,
        UPPER(carrier) AS carrier_upper
    FROM filtered
    WHERE carrier IS DISTINCT FROM 'UPS'
),
enriched AS (
    SELECT
        id,
        order_id,
        carrier,
        cost,
        carrier_filled,
        shipping_fee,
        order_date_label,
        carrier_upper,
        NTILE(3) OVER (PARTITION BY order_id, carrier ORDER BY shipping_fee DESC, id) AS shipping_fee_bucket,
        STDDEV_SAMP(is_lost) OVER (PARTITION BY order_id) AS order_id_stddev_samp_is_lost,
        LEAD(shipping_fee, 1) OVER (PARTITION BY order_id ORDER BY is_lost, id) AS next_shipping_fee
    FROM valid
)
SELECT
    id,
    carrier,
    cost,
    order_date_label,
    shipping_fee_bucket,
    order_id_stddev_samp_is_lost,
    next_shipping_fee
FROM enriched;
