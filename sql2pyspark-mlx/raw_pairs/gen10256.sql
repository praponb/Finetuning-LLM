WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(revenue) OVER (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_revenue,
        MAX(revenue) OVER (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_revenue,
        MIN(revenue) OVER (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_revenue
    FROM store_sales
)
SELECT
    id,
    sale_date,
    units,
    revenue,
    last4_avg_revenue,
    last4_min_revenue
FROM enriched
ORDER BY id NULLS FIRST
