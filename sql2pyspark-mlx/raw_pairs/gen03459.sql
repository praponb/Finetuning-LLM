WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        units,
        revenue,
        revenue + units AS revenue_per_unit,
        DATE_FORMAT(sale_date, 'yyyy-MM-dd') AS sale_date_label
    FROM store_sales
    WHERE sale_date > DATE '2023-08-30'
),
joined AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.units,
        s.revenue,
        s.revenue_per_unit,
        b.cost,
        b.price
    FROM valid s
    LEFT JOIN products b
        ON s.product_id = b.id
    WHERE b.price BETWEEN 410 AND 500
),
enriched AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.units,
        s.revenue_per_unit,
        s.cost,
        p.brand
    FROM joined s
    LEFT JOIN products p
        ON s.product_id = p.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand, product_id ORDER BY units ASC NULLS LAST, id ASC) AS dedup_rank
        FROM enriched
    ) t
    WHERE dedup_rank = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY product_id ORDER BY units ASC, id ASC) AS units_bucket,
        SUM(cost) OVER (ORDER BY revenue_per_unit ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost
    FROM latest
)
-- Window metrics over several different partitions
SELECT
    *,
    AVG(revenue_per_unit) OVER (PARTITION BY product_id ORDER BY running_cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_revenue_per_unit,
    SUM(revenue_per_unit) OVER (PARTITION BY product_id ORDER BY running_cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_revenue_per_unit,
    MIN(revenue_per_unit) OVER (PARTITION BY product_id ORDER BY running_cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_revenue_per_unit
FROM metrics
