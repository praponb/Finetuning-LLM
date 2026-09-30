WITH remaining_keys AS (
    SELECT customer_id, channel
    FROM orders
    EXCEPT
    SELECT customer_id, channel
    FROM orders
    WHERE customer_id IN (11, 28, 33)
),
reconciled AS (
    SELECT s.*
    FROM orders s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.channel = k.channel
),
with_windows AS (
    SELECT
        *,
        CORR(shipping_fee, discount) OVER (PARTITION BY store_id, status) AS corr_shipping_fee_discount,
        CORR(shipping_fee, discount) OVER (PARTITION BY channel) AS corr_shipping_fee_discount_2,
        COUNT(discount) OVER (PARTITION BY status) AS status_count_discount,
        STDDEV_SAMP(amount) OVER (PARTITION BY status) AS status_stddev_samp_amount
    FROM reconciled
),
with_windows_orders AS (
    SELECT
        *,
        CORR(discount, amount) OVER w_recent AS corr_discount_amount,
        SUM(corr_shipping_fee_discount) OVER (PARTITION BY customer_id) AS customer_id_sum_corr_shipping_fee_discount,
        STDDEV_SAMP(corr_shipping_fee_discount_2) OVER w_recent AS status_stddev_samp_corr_shipping_fee_discount_2,
        CORR(shipping_fee, amount) OVER w_running AS corr_shipping_fee_amount
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY status), w_running AS (PARTITION BY status, customer_id)
),
enriched AS (
    SELECT
        *,
        MAX(corr_shipping_fee_discount) OVER (PARTITION BY status) AS status_max_corr_shipping_fee_discount,
        DENSE_RANK() OVER (ORDER BY corr_shipping_fee_discount_2 DESC NULLS LAST) AS corr_shipping_fee_discount_2_dense_rank,
        RANK() OVER (ORDER BY amount DESC) AS amount_rank
    FROM with_windows_orders
),
enriched_orders AS (
    SELECT
        *,
        COUNT(corr_shipping_fee_discount_2_dense_rank) OVER (PARTITION BY store_id, status) AS store_id_count_corr_shipping_fee_discount_2_dense_rank,
        PERCENT_RANK() OVER (ORDER BY status_stddev_samp_corr_shipping_fee_discount_2 DESC NULLS FIRST) AS status_stddev_samp_corr_shipping_fee_discount_2_percent_rank,
        CUME_DIST() OVER (PARTITION BY store_id ORDER BY shipping_fee NULLS LAST) AS shipping_fee_cume_dist
    FROM enriched
),
daily AS (
    SELECT
        CASE WHEN corr_shipping_fee_discount_2 >= 0.22 THEN 'HIGH' ELSE 'LOW' END AS corr_shipping_fee_discount_2_tier,
        CASE WHEN store_id_count_corr_shipping_fee_discount_2_dense_rank >= 16 THEN 'LARGE' ELSE 'SMALL' END AS store_id_count_corr_shipping_fee_discount_2_dense_rank_tier,
        customer_id,
        COUNT(*) FILTER (WHERE channel = 'partner') AS partner_rows,
        AVG(status_count_discount) AS avg_status_count_discount,
        ROUND(SUM(corr_discount_amount) / NULLIF(COUNT(CASE WHEN channel = 'mobile' THEN 1 END), 0), 2) AS corr_discount_amount_per_mobile
    FROM enriched_orders
    GROUP BY GROUPING SETS ((CASE WHEN corr_shipping_fee_discount_2 >= 0.22 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN store_id_count_corr_shipping_fee_discount_2_dense_rank >= 16 THEN 'LARGE' ELSE 'SMALL' END, customer_id), (CASE WHEN corr_shipping_fee_discount_2 >= 0.22 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN store_id_count_corr_shipping_fee_discount_2_dense_rank >= 16 THEN 'LARGE' ELSE 'SMALL' END), ())
    HAVING COUNT(*) FILTER (WHERE channel = 'partner') > 1
)
SELECT
    corr_shipping_fee_discount_2_tier,
    store_id_count_corr_shipping_fee_discount_2_dense_rank_tier,
    customer_id,
    avg_status_count_discount,
    corr_discount_amount_per_mobile
FROM daily
ORDER BY corr_shipping_fee_discount_2_tier DESC NULLS FIRST
