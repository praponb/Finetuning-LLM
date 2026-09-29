WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        CONCAT(city, ', ', region) AS city_region,
        CONCAT(city, ', ', region) AS city_region_2,
        COALESCE(region, 'OTHER') AS resolved_region
    FROM stores
    WHERE opened_date >= DATE '2023-07-21'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC NULLS FIRST, id) AS rn
        FROM valid
    ) ranked
    WHERE rn = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.product_id IN (2, 7, 8, 10)
        )
),
with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.revenue,
        top1.store_sale_sale_date
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.revenue, c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(revenue, 1) OVER (PARTITION BY city ORDER BY revenue, id) AS prev_revenue,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY revenue DESC) AS revenue_percent_rank,
        DENSE_RANK() OVER w_ordered AS revenue_dense_rank,
        SUM(sqft) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft
    FROM with_last
    WINDOW w_ordered AS (PARTITION BY region ORDER BY revenue DESC NULLS FIRST)
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.store_sale_sale_date_2
    FROM enriched s
    JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date_2
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        city,
        region,
        opened_date,
        city_region,
        resolved_region,
        rn,
        revenue,
        store_sale_sale_date,
        prev_revenue,
        revenue_percent_rank,
        revenue_dense_rank,
        cumulative_sqft,
        LEAST(rn, cumulative_sqft) AS least_rn_cumulative_sqft
    FROM latest_match
    WHERE cumulative_sqft = 32161
)
SELECT
    id,
    city,
    opened_date,
    resolved_region,
    rn,
    prev_revenue,
    revenue_percent_rank,
    revenue_dense_rank,
    cumulative_sqft
FROM filtered
ORDER BY prev_revenue DESC NULLS LAST, revenue_dense_rank
