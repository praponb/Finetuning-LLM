WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        discount + amount AS discount_plus_amount
    FROM orders
    WHERE amount < 920
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY order_date, id) AS rank_in_group
    FROM filtered
    QUALIFY rank_in_group = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(discount) OVER (PARTITION BY status, store_id) AS status_stddev_samp_discount,
        COUNT(shipping_fee) OVER (PARTITION BY channel) AS channel_count_shipping_fee,
        SUM(discount_plus_amount) OVER (ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_plus_amount,
        FIRST_VALUE(channel, TRUE) OVER (ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel
    FROM latest
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM metrics src
    JOIN promotions pr
        ON src.channel <=> pr.channel
       AND src.order_date BETWEEN pr.starts_on AND pr.ends_on
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY promo_code, status ORDER BY discount_plus_amount DESC, id, promo_id) AS seq_no,
        ROUND(channel_count_shipping_fee / SUM(channel_count_shipping_fee) OVER (PARTITION BY channel, store_id), 4) AS channel_count_shipping_fee_share,
        ROW_NUMBER() OVER (ORDER BY discount_plus_amount, id, promo_id) AS seq_no_2,
        LEAD(cumulative_discount_plus_amount, 1) OVER w_hist AS next_cumulative_discount_plus_amount
    FROM attributed
    WINDOW w_hist AS (PARTITION BY channel ORDER BY discount_pct, id, promo_id)
),
attributed_orders AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM scored e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.order_date >= pr.starts_on
       AND e.order_date < pr.ends_on + INTERVAL 7 DAYS
),
totals AS (
    -- Grouped metrics
    SELECT
        promo_code,
        CASE WHEN discount_plus_amount >= 74 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_plus_amount_tier,
        ROUND(SUM(seq_no_2) / NULLIF(COUNT(CASE WHEN status = 'RETURNED' THEN 1 END), 0), 2) AS seq_no_2_per_returned,
        COUNT(*) AS num_rows,
        GROUPING_ID() AS grouping_id
    FROM attributed_orders
    GROUP BY GROUPING SETS ((promo_code, CASE WHEN discount_plus_amount >= 74 THEN 'PREMIUM' ELSE 'STANDARD' END), (promo_code))
)
SELECT
    promo_code,
    discount_plus_amount_tier,
    num_rows,
    grouping_id
FROM totals
ORDER BY grouping_id DESC, discount_plus_amount_tier
