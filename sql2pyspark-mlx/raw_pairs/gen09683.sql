WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(discount) OVER (ORDER BY order_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS discount_avg_5,
        CORR(discount, amount) OVER (PARTITION BY status, customer_id) AS corr_discount_amount
    FROM orders
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        order_date,
        status,
        channel,
        discount_avg_5,
        corr_discount_amount
    FROM (
        SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY discount_avg_5 NULLS LAST) AS rn
        FROM metrics
    ) x
    WHERE rn = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_FORMAT(order_date, 'yyyy-MM-dd') AS order_date_label,
        CASE WHEN status IN ('SHIPPED', 'CANCELLED') THEN 'Core' ELSE 'Other' END AS status_group,
        COALESCE(channel, 'DIRECT') AS resolved_channel
    FROM ranked
    WHERE channel IS DISTINCT FROM 'mobile'
      AND order_date >= DATE '2023-08-18'
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM cleaned src
    LEFT JOIN promotions pr
        ON src.channel <=> pr.channel
       AND src.order_date >= pr.starts_on
       AND src.order_date < pr.ends_on + INTERVAL '7' DAY
)
SELECT
    id,
    order_date,
    status,
    channel,
    discount_avg_5,
    order_date_label,
    resolved_channel,
    promo_id,
    promo_code
FROM matched;
