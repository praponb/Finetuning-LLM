WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN amount >= 1230 THEN 'High' WHEN amount >= 720 THEN 'Medium' ELSE 'Low' END AS amount_band
    FROM payments
    WHERE order_id IS NOT NULL
      AND method = 'bank_transfer'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        amount_band
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount NULLS LAST, id) = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC NULLS FIRST, id) AS rank_in_group
    FROM deduped
    QUALIFY rank_in_group = 1
),
joined AS (
    -- Attach orders attributes
    SELECT a.*, d.order_date
    FROM latest a
    JOIN orders d
        ON a.order_id = d.id
    WHERE d.discount <= 0.22
),
enriched AS (
    -- Attach orders attributes
    SELECT
        a.*,
        b.status AS order_status,
        b.shipping_fee,
        b.amount AS order_amount
    FROM joined a
    LEFT JOIN orders b
        ON a.order_id = b.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(shipping_fee, 2, 0) OVER (PARTITION BY order_status ORDER BY shipping_fee, id) AS next_shipping_fee,
        LEAD(order_amount, 1) OVER w_part AS next_order_amount,
        ROW_NUMBER() OVER w_hist AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY order_date DESC, id) AS event_seq,
        SUM(shipping_fee) OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_shipping_fee
    FROM enriched
    WINDOW w_part AS (PARTITION BY YEAR(order_date) ORDER BY amount, id), w_hist AS (PARTITION BY order_status ORDER BY order_amount DESC, id)
)
SELECT
    id,
    order_id,
    method,
    amount,
    status,
    amount_band,
    order_date,
    order_status,
    next_shipping_fee
FROM scored
