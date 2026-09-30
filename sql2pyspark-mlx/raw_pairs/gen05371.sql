WITH cleaned AS (
    SELECT
        *,
        LOWER(channel) AS channel_lower,
        CEIL(shipping_fee) AS shipping_fee_whole
    FROM orders
    WHERE channel IS NOT NULL
      AND order_date > TO_DATE('2023-03-23')
),
matched AS (
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM cleaned s
    JOIN promotions pr
        ON COALESCE(s.channel, 'store') = pr.channel
       AND s.order_date >= pr.starts_on
       AND s.order_date < pr.ends_on + INTERVAL '30' DAY
),
eligible AS (
    SELECT s.*
    FROM matched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.order_id = s.id
              AND x.product_id IS NOT NULL
        )
),
matched_orders AS (
    SELECT
        src.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM eligible src
    JOIN promotions pr
        ON COALESCE(src.channel, 'store') = pr.channel
       AND src.order_date BETWEEN pr.starts_on AND pr.ends_on
),
summary AS (
    SELECT
        DATE_TRUNC('YEAR', order_date) AS year_start,
        COUNT_IF(promotion_promo_code = 'SPRING10') AS spring10_events,
        COUNT(*) AS row_count,
        SORT_ARRAY(COLLECT_SET(status)) AS status_list,
        COUNT_IF(status = 'RETURNED') AS returned_events
    FROM matched_orders
    GROUP BY year_start
)
SELECT
    year_start,
    spring10_events,
    status_list
FROM summary
ORDER BY year_start DESC, spring10_events DESC;
