WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        CAST(shipping_fee AS INT) AS shipping_fee_int,
        amount * discount AS amount_times_discount
    FROM orders
    WHERE customer_id IS NOT NULL
      AND store_id IN (20, 34, 36)
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM cleaned a
    LEFT JOIN promotions pr
        ON a.channel <=> pr.channel
       AND a.order_date >= pr.starts_on
       AND a.order_date < pr.ends_on + INTERVAL 30 DAYS
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(discount_pct) OVER w_part AS running_count_discount_pct,
        AVG(discount_pct) OVER w_part AS running_avg_discount_pct,
        MIN(discount_pct) OVER w_part AS running_min_discount_pct,
        LEAD(shipping_fee_int, 1) OVER (PARTITION BY store_id, promo_code ORDER BY discount_pct, id, promo_id) AS next_shipping_fee_int
    FROM matched
    WINDOW w_part AS (PARTITION BY status ORDER BY amount, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
matched_orders AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM enriched src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.order_date >= sub.start_date
       AND src.order_date <= DATE_ADD(sub.end_date, 7)
),
cleaned_orders AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN amount_times_discount >= 88 THEN 'High' WHEN amount_times_discount >= 30 THEN 'Medium' ELSE 'Low' END AS amount_times_discount_tier,
        COALESCE(promo_code, 'DIRECT') AS promo_code_filled
    FROM matched_orders
    WHERE order_date >= DATE '2023-07-17'
      AND status = 'COMPLETED'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(shipping_fee_int) OVER w_part AS cumulative_shipping_fee_int,
        LEAD(amount, 1) OVER w_ordered AS next_amount,
        ROUND(subscription_id / SUM(subscription_id) OVER (PARTITION BY MONTH(order_date), status), 4) AS subscription_id_share
    FROM cleaned_orders
    WINDOW w_part AS (PARTITION BY channel ORDER BY amount_times_discount, id, promo_id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (ORDER BY discount_pct, id, promo_id, subscription_id)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', order_date) AS week_start,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
    SUM(amount_times_discount) FILTER (WHERE status = 'COMPLETED') AS completed_total_amount_times_discount,
    ROUND(SUM(running_avg_discount_pct) / NULLIF(COUNT(CASE WHEN plan = 'premium' THEN 1 END), 0), 2) AS running_avg_discount_pct_per_premium,
    COUNT(*) FILTER (WHERE plan = 'enterprise') AS enterprise_rows,
    SUM(CASE WHEN plan = 'enterprise' THEN next_amount ELSE 0 END) AS enterprise_next_amount,
    MAX(cumulative_shipping_fee_int) FILTER (WHERE channel = 'mobile') AS mobile_max_cumulative_shipping_fee_int
FROM scored
GROUP BY week_start
HAVING COUNT(*) FILTER (WHERE plan = 'enterprise') > 1
