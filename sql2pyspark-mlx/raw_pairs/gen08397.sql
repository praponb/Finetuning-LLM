WITH base AS (
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        status,
        DATEDIFF(CURRENT_DATE(), shipped_date) / 365 AS years_since_shipped_date,
        COALESCE(carrier, 'UNKNOWN') AS carrier_filled
    FROM shipments
    WHERE shipped_date >= TO_DATE('2023-06-21')
      AND order_id IS NOT NULL
),
with_dims AS (
    SELECT
        s.id,
        s.order_id,
        s.delivered_date,
        s.status,
        s.years_since_shipped_date,
        s.carrier_filled,
        b.order_date,
        b.shipping_fee
    FROM base s
    LEFT JOIN orders b
        ON s.order_id = b.id
),
metrics AS (
    SELECT
        id,
        order_id,
        carrier_filled,
        order_date,
        shipping_fee,
        NTILE(3) OVER (PARTITION BY status, order_id ORDER BY shipping_fee DESC, id) AS shipping_fee_bucket,
        CUME_DIST() OVER (PARTITION BY status, order_id ORDER BY shipping_fee DESC NULLS LAST) AS shipping_fee_cume_dist,
        SUM(years_since_shipped_date) OVER (PARTITION BY status ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_years_since_shipped_date,
        NTILE(3) OVER (PARTITION BY status ORDER BY years_since_shipped_date DESC, id) AS years_since_shipped_date_bucket
    FROM with_dims
    WHERE status = 'DELIVERED'
),
with_dims_shipments AS (
    SELECT
        s.*,
        b.status,
        b.discount,
        b.channel
    FROM metrics s
    JOIN orders b
        ON s.order_id <=> b.id
),
latest AS (
    SELECT *
    FROM with_dims_shipments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount NULLS LAST, id) = 1
),
with_windows AS (
    SELECT
        id,
        order_id,
        shipping_fee,
        shipping_fee_cume_dist,
        years_since_shipped_date_bucket,
        status,
        discount,
        MIN(discount) OVER (PARTITION BY channel ORDER BY shipping_fee_bucket, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_discount,
        COUNT(discount) OVER (PARTITION BY channel ORDER BY shipping_fee_bucket, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_discount,
        SUM(discount) OVER (PARTITION BY channel ORDER BY shipping_fee_bucket, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_discount,
        AVG(discount) OVER (PARTITION BY channel ORDER BY shipping_fee_bucket, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_discount
    FROM latest
)
SELECT
    id,
    order_id,
    shipping_fee,
    shipping_fee_cume_dist,
    years_since_shipped_date_bucket,
    status,
    discount,
    last3_count_discount,
    last3_sum_discount,
    CASE WHEN status RLIKE '^(RETURNED|COMPLETED)$' THEN 'PRIORITY' WHEN status RLIKE '^(PENDING|CANCELLED)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS status_zone
FROM with_windows
ORDER BY years_since_shipped_date_bucket NULLS FIRST;
