WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_FORMAT(order_date, 'MMM yyyy') AS order_date_label,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM orders
    WHERE channel IS DISTINCT FROM 'mobile'
)
-- Non-equi join on a validity window
SELECT
    e.*,
    pr.id AS promo_id,
    pr.promo_code,
    pr.discount_pct
FROM filtered e
LEFT JOIN promotions pr
    ON e.channel <=> pr.channel
   AND e.order_date >= pr.starts_on
   AND e.order_date <= DATE_ADD(pr.ends_on, 7);
