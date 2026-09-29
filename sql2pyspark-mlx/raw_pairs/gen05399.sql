WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        ROUND(sqft, 0) sqft_rounded,
        UPPER(TRIM(store_name)) store_name_normalized
    FROM stores
    WHERE region IS DISTINCT FROM 'Central'
      AND opened_date >= DATE '2023-08-07'
),
latest_match AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.city,
        s.region,
        s.sqft_rounded,
        s.store_name_normalized,
        latest.order_order_date
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.order_date AS order_order_date
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        city,
        region,
        store_name_normalized,
        SUM(sqft_rounded) OVER w_hist running_sqft_rounded,
        SUM(sqft_rounded) OVER (PARTITION BY region ORDER BY order_order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sqft_rounded_2,
        NTILE(4) OVER (PARTITION BY city ORDER BY sqft_rounded DESC, id) sqft_rounded_bucket,
        LAG(sqft_rounded, 2) OVER (PARTITION BY MONTH(order_order_date) ORDER BY order_order_date, id) prev_sqft_rounded
    FROM latest_match
    WINDOW w_hist AS (PARTITION BY region ORDER BY sqft_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.channel <> 'mobile'
        )
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY city ORDER BY prev_sqft_rounded, id) prev_sqft_rounded_bucket,
        NTILE(10) OVER (ORDER BY running_sqft_rounded_2 DESC, id) running_sqft_rounded_2_bucket,
        NTILE(4) OVER (PARTITION BY city ORDER BY running_sqft_rounded DESC, id) running_sqft_rounded_bucket,
        ROUND(sqft_rounded_bucket / SUM(sqft_rounded_bucket) OVER (PARTITION BY city), 4) sqft_rounded_bucket_share,
        SUM(running_sqft_rounded) OVER (ORDER BY running_sqft_rounded_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_running_sqft_rounded
    FROM kept
    WHERE sqft_rounded_bucket >= 5
)
SELECT
    id,
    city,
    store_name_normalized,
    running_sqft_rounded,
    running_sqft_rounded_2,
    sqft_rounded_bucket,
    prev_sqft_rounded,
    running_sqft_rounded_2_bucket,
    running_sqft_rounded_bucket,
    CASE WHEN city RLIKE '^(Tokyo|London)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS city_zone
FROM with_windows
ORDER BY city;
