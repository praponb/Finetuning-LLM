WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(revenue * 1.15, 2) AS revenue_adjusted
    FROM store_sales
    WHERE product_id IN (2, 5, 7, 9)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue DESC, id ASC) AS rn
        FROM valid
    ) ranked
    WHERE rn = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        sale_date,
        revenue,
        revenue_adjusted,
        rn,
        DENSE_RANK() OVER w_ordered AS revenue_dense_rank,
        AVG(revenue) OVER (PARTITION BY store_id) AS store_id_avg_revenue
    FROM deduped
    WINDOW w_ordered AS (ORDER BY revenue ASC NULLS FIRST)
),
deduped_store_sales AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY store_id_avg_revenue DESC NULLS LAST, id ASC) = 1
)
SELECT
    id,
    product_id,
    sale_date,
    revenue,
    revenue_adjusted,
    rn
FROM deduped_store_sales;
