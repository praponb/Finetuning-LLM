WITH filtered AS (
    SELECT
        id,
        store_id,
        order_date,
        amount,
        discount,
        CASE WHEN shipping_fee >= 41 THEN 'High' WHEN shipping_fee >= 11 THEN 'Medium' ELSE 'Low' END shipping_fee_tier
    FROM orders
    WHERE order_date >= DATE '2023-03-27'
      AND channel IS NOT NULL
),
filtered_orders AS (
    SELECT *, discount - amount discount_minus_amount
    FROM filtered
    WHERE amount IS NULL
),
joined AS (
    SELECT
        e.*,
        b.sqft,
        b.region,
        b.opened_date
    FROM filtered_orders e
    LEFT JOIN stores b
        ON e.store_id = b.id
    WHERE b.sqft = 6943
),
with_windows AS (
    SELECT
        *,
        MAX(discount_minus_amount) OVER (PARTITION BY store_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_discount_minus_amount,
        COUNT(discount_minus_amount) OVER (PARTITION BY store_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_discount_minus_amount,
        AVG(discount_minus_amount) OVER (PARTITION BY store_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_discount_minus_amount,
        MIN(discount_minus_amount) OVER (PARTITION BY store_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_discount_minus_amount,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY store_id, region), 4) sqft_share
    FROM joined
),
matched_keys AS (
    SELECT store_id, id
    FROM with_windows
    INTERSECT ALL
    SELECT store_id, id
    FROM orders
    WHERE store_id = 6
),
kept_rows AS (
    SELECT s.*
    FROM with_windows s
    JOIN matched_keys k ON s.store_id = k.store_id AND s.id = k.id
),
stats AS (
    SELECT
        store_id,
        DATE_TRUNC('YEAR', opened_date) AS year_start,
        PERCENTILE_APPROX(sqft_share, ARRAY(0.25, 0.5, 0.75)) AS sqft_share_quartiles,
        PERCENTILE_APPROX(sqft_share, ARRAY(0.25, 0.5, 0.75)) AS sqft_share_quartiles_2,
        COUNT_IF(region = 'South') AS south_events,
        SORT_ARRAY(COLLECT_SET(region)) AS region_list,
        PERCENTILE_APPROX(sqft_share, ARRAY(0.25, 0.5, 0.75)) AS sqft_share_quartiles_3,
        COUNT(DISTINCT id) AS distinct_ids,
        GROUPING(store_id) AS gid
    FROM kept_rows
    GROUP BY CUBE (store_id, DATE_TRUNC('YEAR', opened_date))
)
SELECT
    store_id,
    year_start,
    sqft_share_quartiles_2,
    south_events,
    sqft_share_quartiles_3,
    distinct_ids
FROM stats
ORDER BY south_events DESC;
