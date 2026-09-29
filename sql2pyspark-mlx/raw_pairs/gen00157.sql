WITH cleaned AS (
    SELECT
        id,
        order_id,
        cost,
        status,
        UPPER(carrier) carrier_upper,
        DATEDIFF(delivered_date, shipped_date) days_shipped_date_to_delivered_date,
        COALESCE(carrier, 'OTHER') carrier_clean
    FROM shipments
    WHERE order_id IS NOT NULL
),
with_dims AS (
    SELECT s.*, d.status AS order_status
    FROM cleaned s
    INNER JOIN orders d
        ON s.order_id <=> d.id
),
with_windows AS (
    SELECT
        id,
        order_id,
        cost,
        status,
        carrier_upper,
        days_shipped_date_to_delivered_date,
        order_status,
        LAG(days_shipped_date_to_delivered_date, 1) OVER (PARTITION BY order_id ORDER BY cost, id) prev_days_shipped_date_to_delivered_date,
        NTILE(3) OVER w_recent cost_bucket
    FROM with_dims
    WHERE days_shipped_date_to_delivered_date <= 63
    WINDOW w_recent AS (PARTITION BY order_id ORDER BY cost DESC, id)
),
latest AS (
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY prev_days_shipped_date_to_delivered_date NULLS LAST, id) = 1
),
cleaned_shipments AS (
    SELECT
        id,
        order_id,
        cost,
        status,
        carrier_upper,
        order_status,
        cost_bucket,
        CASE WHEN status = 'RETURNED' THEN 1 ELSE 0 END is_returned,
        COALESCE(order_status, 'DIRECT') order_status_clean
    FROM latest
    WHERE status IN ('DELIVERED', 'IN_TRANSIT', 'RETURNED')
)
SELECT *
FROM (
    SELECT
        order_status,
        status,
        cost_bucket
    FROM cleaned_shipments
) src
PIVOT (
    AVG(cost_bucket) AS amount
    FOR status IN ('LOST' AS lost, 'RETURNED' AS returned, 'DELIVERED' AS delivered)
)
