WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(units) OVER (PARTITION BY product_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_units,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY store_id), 4) revenue_share
    FROM store_sales
)
-- Multi-level aggregation
SELECT
    CASE WHEN revenue_share >= 0.62 THEN 'PREMIUM' ELSE 'STANDARD' END AS revenue_share_tier,
    DATE_TRUNC('WEEK', sale_date) AS week_start,
    CORR(units, revenue) AS corr_units_revenue,
    COUNT(*) AS event_count,
    CORR(revenue, units) AS corr_revenue_units
FROM enriched
GROUP BY 1, 2
ORDER BY revenue_share_tier DESC NULLS LAST;
