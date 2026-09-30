WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), sale_date) / 30 AS months_since_sale_date,
        FLOOR(revenue) AS revenue_whole
    FROM store_sales
    WHERE units > 18
),
joined AS (
    -- Attach stores attributes
    SELECT
        e.*,
        d.city,
        d.store_name,
        d.opened_date
    FROM base e
    LEFT JOIN stores d
        ON e.store_id = d.id
    WHERE d.region = 'North'
),
enriched AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.store_id,
        a.revenue_whole,
        a.store_name,
        a.opened_date,
        d.sqft
    FROM joined a
    LEFT JOIN stores d
        ON a.store_id = d.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        revenue_whole,
        store_name,
        opened_date,
        MIN(revenue_whole) OVER w_ordered AS last4_min_revenue_whole,
        COUNT(revenue_whole) OVER w_ordered AS last4_count_revenue_whole,
        MAX(revenue_whole) OVER w_ordered AS last4_max_revenue_whole,
        AVG(revenue_whole) OVER w_ordered AS last4_avg_revenue_whole,
        ROW_NUMBER() OVER w_hist AS event_seq
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY store_id ORDER BY revenue_whole, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY YEAR(opened_date), store_id ORDER BY revenue_whole DESC, id)
),
enriched_store_sales AS (
    -- Running and moving aggregates
    SELECT
        id,
        last4_min_revenue_whole,
        last4_count_revenue_whole,
        last4_avg_revenue_whole,
        DENSE_RANK() OVER (ORDER BY last4_count_revenue_whole DESC) AS last4_count_revenue_whole_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY MONTH(opened_date) ORDER BY revenue_whole, id) AS row_seq
    FROM scored
)
-- Grouped metrics
SELECT
    CASE WHEN last4_count_revenue_whole >= 14 THEN 'LARGE' ELSE 'SMALL' END AS last4_count_revenue_whole_tier,
    PERCENTILE_APPROX(last4_count_revenue_whole_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS last4_count_revenue_whole_dense_rank_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    GROUPING_ID() AS grouping_id
FROM enriched_store_sales
GROUP BY ROLLUP (CASE WHEN last4_count_revenue_whole >= 14 THEN 'LARGE' ELSE 'SMALL' END)
HAVING COUNT(DISTINCT id) >= 1
ORDER BY distinct_ids DESC;
