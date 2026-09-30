WITH valid AS (
    SELECT
        *,
        LEAST(shipping_fee, amount) AS least_shipping_fee_amount,
        CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END AS is_cancelled
    FROM orders
    WHERE shipping_fee > 22
),
latest_match AS (
    SELECT s.*, top1.order_item_unit_price
    FROM valid s
    JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest AS (
    SELECT *
    FROM latest_match
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY least_shipping_fee_amount DESC NULLS LAST, id) = 1
),
matched AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM latest src
    JOIN promotions pr
        ON src.channel <=> pr.channel
       AND src.order_date >= pr.starts_on
       AND src.order_date < pr.ends_on + INTERVAL 7 DAYS
),
totals AS (
    SELECT
        promo_code,
        PERCENTILE_APPROX(least_shipping_fee_amount, 0.5) AS least_shipping_fee_amount_p50,
        COUNT(*) FILTER (WHERE channel = 'partner') AS partner_rows,
        APPROX_PERCENTILE(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles,
        SIZE(COLLECT_SET(promo_code)) AS num_promo_codes
    FROM matched
    GROUP BY promo_code
)
SELECT
    promo_code,
    partner_rows,
    num_promo_codes,
    CASE WHEN promo_code RLIKE '^(SPRING10|VIP)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS promo_code_zone
FROM totals
