WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        units,
        revenue,
        units - revenue AS revenue_per_unit,
        CAST(revenue AS INT) AS revenue_int
    FROM store_sales
    WHERE revenue >= 3300
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY product_id, store_id ORDER BY revenue DESC) AS dedup_rank
    FROM base
    QUALIFY dedup_rank = 1
),
joined AS (
    -- Attach stores attributes
    SELECT e.*, s.sqft
    FROM ranked e
    JOIN stores s
        ON e.store_id = s.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue DESC NULLS FIRST, id) AS dedup_rank_2
    FROM joined
    QUALIFY dedup_rank_2 = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(revenue) OVER (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_revenue,
        MIN(revenue) OVER (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_revenue,
        SUM(revenue) OVER (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_revenue,
        SUM(units) OVER (PARTITION BY product_id ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_units
    FROM latest
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN running_sum_revenue >= 17700 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_sum_revenue_tier,
        CASE WHEN revenue_int >= 61 THEN 'HIGH' ELSE 'LOW' END AS revenue_int_tier,
        BIT_OR(units) AS units_bit_or,
        MIN(cumulative_units) AS min_cumulative_units,
        APPROX_PERCENTILE(revenue_int, 0.95) AS revenue_int_p95,
        CORR(units, revenue) AS corr_units_revenue,
        APPROX_PERCENTILE(running_min_revenue, ARRAY(0.25, 0.5, 0.75)) AS running_min_revenue_quartiles,
        COUNT(DISTINCT id) AS distinct_ids,
        GROUPING(CASE WHEN running_sum_revenue >= 17700 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_id
    FROM enriched
    GROUP BY GROUPING SETS ((CASE WHEN running_sum_revenue >= 17700 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN revenue_int >= 61 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN running_sum_revenue >= 17700 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
    HAVING revenue_int_p95 >= 0
)
SELECT
    running_sum_revenue_tier,
    revenue_int_tier,
    units_bit_or,
    running_min_revenue_quartiles,
    distinct_ids,
    grouping_id
FROM totals
ORDER BY units_bit_or, grouping_id
