WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        amount + shipping_fee AS amount_with_shipping,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM orders
    WHERE customer_id IS NOT NULL
)
-- Window metrics over several different partitions
SELECT
    id,
    customer_id,
    store_id,
    order_date,
    status,
    channel,
    amount,
    discount,
    shipping_fee,
    amount_with_shipping,
    resolved_channel,
    AVG(amount_with_shipping) OVER (PARTITION BY store_id ORDER BY amount_with_shipping ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS amount_with_shipping_avg_3_prior,
    AVG(discount) OVER w_ordered AS discount_avg_3,
    NTILE(3) OVER (ORDER BY amount_with_shipping ASC, id ASC) AS amount_with_shipping_bucket,
    NTILE(3) OVER (PARTITION BY customer_id, store_id ORDER BY shipping_fee DESC, id ASC) AS shipping_fee_bucket,
    AVG(shipping_fee) OVER (ORDER BY order_date ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS shipping_fee_avg_5_prior
FROM filtered
WINDOW w_ordered AS (ORDER BY discount ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);
