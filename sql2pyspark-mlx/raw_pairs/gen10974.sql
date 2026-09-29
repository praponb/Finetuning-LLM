WITH base AS (
    SELECT *, LENGTH(status) AS status_length
    FROM shipments
    WHERE carrier IS NOT NULL
      AND carrier IS DISTINCT FROM 'UPS'
),
enriched AS (
    SELECT
        e.id,
        e.order_id,
        e.shipped_date,
        e.status,
        e.status_length,
        d.amount,
        d.shipping_fee
    FROM base e
    JOIN orders d
        ON e.order_id <=> d.id
),
scored AS (
    SELECT
        *,
        LAG(status_length, 2, 0) OVER (PARTITION BY order_id ORDER BY status_length ASC, id ASC) AS prev_status_length,
        COUNT(amount) OVER (PARTITION BY status) AS status_count_amount,
        SUM(status_length) OVER (PARTITION BY status, order_id ORDER BY shipping_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_length
    FROM enriched
    WHERE amount <= 480
),
enriched_shipments AS (
    SELECT
        id,
        status,
        shipping_fee,
        status_count_amount,
        SUM(running_status_length) OVER (PARTITION BY order_id) AS order_id_sum_running_status_length,
        STDDEV_SAMP(running_status_length) OVER w_recent AS order_id_stddev_samp_running_status_length,
        ROW_NUMBER() OVER w_grp AS seq_no,
        ROUND(status_count_amount / SUM(status_count_amount) OVER (PARTITION BY YEAR(shipped_date), status), 4) AS status_count_amount_share,
        NTILE(4) OVER (PARTITION BY order_id, status ORDER BY status_count_amount DESC, id ASC) AS status_count_amount_bucket
    FROM scored
    WINDOW w_recent AS (PARTITION BY order_id, status), w_grp AS (PARTITION BY order_id ORDER BY shipped_date ASC, id ASC)
)
SELECT *
FROM (
    SELECT
        id,
        status,
        status_count_amount_share,
        shipping_fee
    FROM enriched_shipments
) src
UNPIVOT (
    val FOR kpi IN (status_count_amount_share, shipping_fee AS SHIPPING_FEE_VAL)
);
