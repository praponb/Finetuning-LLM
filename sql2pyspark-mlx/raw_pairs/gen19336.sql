WITH filtered AS (
    SELECT *, LAST_DAY(shipped_date) AS shipped_date_month_end
    FROM shipments
    WHERE shipped_date > TO_DATE('2023-06-16')
      AND carrier IS NOT NULL
),
metrics AS (
    SELECT
        *,
        LEAD(cost) OVER (PARTITION BY carrier, status ORDER BY shipped_date, id) AS next_cost,
        LAG(cost) OVER (PARTITION BY carrier, status ORDER BY shipped_date, id) AS prev_cost,
        ROW_NUMBER() OVER (PARTITION BY carrier, status ORDER BY shipped_date, id) AS seq_no
    FROM filtered
),
cleaned AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        status,
        shipped_date_month_end,
        next_cost,
        seq_no,
        LAST_DAY(delivered_date) AS delivered_date_month_end,
        DATEDIFF(delivered_date, shipped_date) AS days_shipped_date_to_delivered_date
    FROM metrics
    WHERE delivered_date > DATE '2023-03-18'
      AND order_id IS NOT NULL
),
enriched AS (
    SELECT
        e.*,
        p.order_date,
        p.channel
    FROM cleaned e
    INNER JOIN orders p
        ON e.order_id = p.id
    WHERE p.amount IS NOT NULL
),
enriched_shipments AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY YEAR(delivered_date) ORDER BY shipped_date DESC, id) AS seq_no_2,
        FIRST_VALUE(seq_no, TRUE) OVER (ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_seq_no
    FROM enriched
),
enriched_shipments_2 AS (
    SELECT
        a.id,
        a.order_id,
        a.carrier,
        a.delivered_date,
        a.status,
        a.seq_no,
        a.delivered_date_month_end,
        a.order_date,
        a.channel,
        a.seq_no_2,
        a.first_known_seq_no,
        b.shipping_fee,
        b.channel AS order_channel,
        b.order_date AS order_order_date
    FROM enriched_shipments a
    LEFT JOIN orders b
        ON a.order_id = b.id
)
SELECT
    id,
    order_id,
    delivered_date,
    status,
    delivered_date_month_end,
    order_date,
    channel,
    first_known_seq_no,
    shipping_fee,
    order_channel,
    order_order_date,
    LAG(seq_no_2, 1) OVER (PARTITION BY status ORDER BY order_date, id) AS prev_seq_no_2,
    DENSE_RANK() OVER (PARTITION BY carrier ORDER BY shipping_fee DESC NULLS LAST) AS shipping_fee_dense_rank
FROM enriched_shipments_2
