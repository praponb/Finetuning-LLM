WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM store_sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY units DESC NULLS FIRST, id) = 1
),
ranked_store_sales AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM ranked
    QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY units) = 1
),
enriched AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.store_id,
        src.product_id,
        src.revenue,
        s.region,
        s.opened_date,
        s.city
    FROM ranked_store_sales src
    LEFT JOIN stores s
        ON src.store_id = s.id
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        revenue,
        region,
        opened_date,
        city
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region, store_id ORDER BY revenue NULLS LAST, id) = 1
)
-- Running and moving aggregates
SELECT
    id,
    revenue,
    city,
    STDDEV_SAMP(revenue) OVER (PARTITION BY city) AS city_stddev_samp_revenue,
    PERCENT_RANK() OVER (PARTITION BY city ORDER BY revenue DESC) AS revenue_percent_rank
FROM deduped
