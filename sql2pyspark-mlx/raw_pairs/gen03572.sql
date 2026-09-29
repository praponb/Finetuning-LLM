WITH cleaned AS (
    SELECT *, CASE WHEN sqft >= 16482 THEN 'High' WHEN sqft >= 4338 THEN 'Medium' ELSE 'Low' END AS sqft_level
    FROM stores
    WHERE opened_date > DATE '2023-04-13'
),
with_latest AS (
    SELECT
        s.*,
        top1.order_order_date,
        top1.shipping_fee
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.order_date AS order_order_date, c.shipping_fee
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
ranked AS (
    SELECT
        id,
        store_name,
        opened_date,
        order_order_date,
        shipping_fee
    FROM with_latest
    QUALIFY RANK() OVER (PARTITION BY region ORDER BY shipping_fee NULLS LAST) = 1
),
latest_match AS (
    SELECT s.*, top1.revenue
    FROM ranked s
    JOIN LATERAL (
            SELECT c.revenue
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.sale_date <= s.order_order_date
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
cleaned_stores AS (
    SELECT
        id,
        store_name,
        order_order_date,
        revenue,
        COALESCE(store_name, 'Unknown') AS store_name_filled
    FROM latest_match
    WHERE opened_date > TO_DATE('2023-09-09')
      AND opened_date < '2025-08-01'
)
SELECT
    CASE WHEN revenue >= 3700 THEN 'LARGE' ELSE 'SMALL' END AS revenue_tier,
    DATE_TRUNC('WEEK', order_order_date) AS week_start,
    APPROX_PERCENTILE(revenue, 0.95) AS revenue_p95,
    PERCENTILE_APPROX(revenue, ARRAY(0.25, 0.5, 0.75)) AS revenue_quartiles,
    COUNT(*) AS row_count,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(revenue, ARRAY(0.25, 0.5, 0.75)) AS revenue_quartiles_2,
    AVG(revenue) AS avg_revenue
FROM cleaned_stores
GROUP BY CASE WHEN revenue >= 3700 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', order_order_date)
