WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        status,
        amount,
        SUM(amount) OVER (PARTITION BY status, customer_id ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        AVG(amount) OVER (PARTITION BY store_id, status ORDER BY order_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS amount_avg_3_prior,
        ROUND(amount / SUM(amount) OVER (PARTITION BY store_id), 4) AS amount_share,
        NTILE(4) OVER w_part AS discount_bucket,
        ROW_NUMBER() OVER w_grp AS seq_no
    FROM orders
    WINDOW w_part AS (PARTITION BY store_id ORDER BY discount DESC, id), w_grp AS (ORDER BY shipping_fee DESC, id)
)
SELECT
    id,
    customer_id,
    status,
    amount,
    discount_bucket,
    seq_no
FROM with_windows
ORDER BY id DESC NULLS LAST, discount_bucket DESC
