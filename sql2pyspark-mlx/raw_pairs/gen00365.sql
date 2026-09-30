WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(carrier) AS carrier_length,
        DATE_FORMAT(delivered_date, 'yyyy-MM-dd') AS delivered_date_label,
        COALESCE(carrier, 'DIRECT') AS carrier_filled
    FROM shipments
    WHERE shipped_date >= DATE '2023-03-22'
      AND shipped_date > '2024-10-01'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY carrier_length DESC) AS dedup_rank
    FROM cleaned
    QUALIFY dedup_rank = 1
),
with_dims AS (
    -- Attach orders attributes
    SELECT a.*, p.status AS order_status
    FROM ranked a
    LEFT JOIN orders p
        ON a.order_id <=> p.id
    WHERE p.store_id = 7
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_recent AS row_seq,
        LEAD(carrier_length, 1) OVER w_grp AS next_carrier_length,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY carrier_length DESC NULLS LAST) AS carrier_length_dense_rank
    FROM with_dims
    WINDOW w_recent AS (PARTITION BY MONTH(shipped_date), status ORDER BY shipped_date, id), w_grp AS (PARTITION BY DATE_TRUNC('MONTH', shipped_date), order_id ORDER BY dedup_rank, id)
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', shipped_date) AS activity_month,
    ROUND(AVG(carrier_length_dense_rank), 2) AS avg_carrier_length_dense_rank,
    SIZE(COLLECT_SET(order_status)) AS num_order_status,
    COUNT_IF(carrier = 'Flash') AS flash_events,
    COUNT_IF(order_status = 'RETURNED') AS returned_events,
    SUM(CASE WHEN carrier = 'FedEx' THEN carrier_length ELSE 0 END) AS fedex_carrier_length,
    COUNT_IF(order_status = 'CANCELLED') AS cancelled_events
FROM scored
GROUP BY 1
ORDER BY fedex_carrier_length
