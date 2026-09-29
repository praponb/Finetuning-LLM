WITH filtered AS (
    SELECT
        id,
        customer_id,
        store_id,
        channel,
        amount,
        shipping_fee,
        UPPER(channel) AS channel_upper
    FROM orders
    WHERE customer_id IS NOT NULL
      AND channel IN ('mobile', 'web', 'partner')
),
valid AS (
    SELECT
        *,
        CASE WHEN channel IN ('mobile', 'partner') THEN 'Tier 1' ELSE 'Other' END AS channel_group,
        FLOOR(shipping_fee) AS shipping_fee_whole,
        COALESCE(channel, 'NONE') AS channel_clean
    FROM filtered
    WHERE channel IS DISTINCT FROM 'partner'
),
joined AS (
    SELECT
        s.id,
        s.customer_id,
        s.store_id,
        s.channel,
        s.shipping_fee,
        s.channel_upper,
        s.shipping_fee_whole,
        p2.opened_date,
        p2.region
    FROM valid s
    LEFT JOIN stores p2
        ON s.store_id = p2.id
),
scored AS (
    SELECT
        *,
        LAG(shipping_fee, 1) OVER (PARTITION BY region ORDER BY shipping_fee, id) AS prev_shipping_fee,
        PERCENT_RANK() OVER (PARTITION BY region ORDER BY shipping_fee_whole DESC) AS shipping_fee_whole_percent_rank
    FROM joined
    WHERE store_id IN (20, 29, 36, 37)
),
eligible AS (
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.unit_price <= 290
        )
)
SELECT
    region,
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    CASE WHEN shipping_fee_whole_percent_rank >= 0.62 THEN 'PREMIUM' ELSE 'STANDARD' END AS shipping_fee_whole_percent_rank_tier,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list,
    AVG(prev_shipping_fee) AS avg_prev_shipping_fee,
    GROUPING_ID() AS grouping_level
FROM eligible
GROUP BY GROUPING SETS ((region, DATE_TRUNC('YEAR', opened_date), CASE WHEN shipping_fee_whole_percent_rank >= 0.62 THEN 'PREMIUM' ELSE 'STANDARD' END), (region, DATE_TRUNC('YEAR', opened_date)), (region), ())
