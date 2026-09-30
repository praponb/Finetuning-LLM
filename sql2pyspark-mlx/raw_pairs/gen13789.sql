WITH valid AS (
    SELECT *, CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END is_pending
    FROM orders
    WHERE order_date > DATE '2023-04-11'
),
matched AS (
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM valid e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.order_date BETWEEN sub.start_date AND sub.end_date
),
with_window AS (
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM matched a
    LEFT JOIN promotions pr
        ON COALESCE(a.channel, 'store') = pr.channel
       AND a.order_date >= pr.starts_on
       AND a.order_date < pr.ends_on + INTERVAL 7 DAYS
),
enriched AS (
    SELECT
        src.*,
        b.city,
        b.store_name
    FROM with_window src
    LEFT JOIN stores b
        ON src.store_id = b.id
),
base AS (
    SELECT
        *,
        TRIM(channel) clean_channel,
        CEIL(monthly_fee) monthly_fee_whole,
        COALESCE(city, 'NONE') city_filled
    FROM enriched
    WHERE order_date > TO_DATE('2023-08-17')
      AND city IS DISTINCT FROM 'London'
)
SELECT
    channel,
    CASE WHEN promo_id >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS promo_id_tier,
    subscription_id,
    SIZE(COLLECT_SET(city)) AS num_cities,
    MAX(amount) AS max_amount,
    COUNT(*) FILTER (WHERE channel = 'mobile') AS mobile_rows,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list
FROM base
GROUP BY 1, 2, 3
ORDER BY num_cities DESC NULLS LAST
