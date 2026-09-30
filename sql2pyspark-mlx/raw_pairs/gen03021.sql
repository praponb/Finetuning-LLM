WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue NULLS LAST, id) AS rn
    FROM store_sales
    QUALIFY rn = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, product_id
    FROM latest
    EXCEPT
    SELECT id, product_id
    FROM store_sales
    WHERE units >= 26
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN kept_keys k ON s.id = k.id AND s.product_id = k.product_id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue,
        rn,
        GREATEST(rn, revenue) greatest_rn_revenue,
        CEIL(units) units_whole
    FROM verified
    WHERE sale_date > TO_DATE('2023-04-15')
      AND product_id IN (4, 7)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        sale_date,
        revenue,
        rn,
        units_whole,
        CASE WHEN units_whole >= 62 THEN 'High' WHEN units_whole >= 26 THEN 'Medium' ELSE 'Low' END units_whole_tier
    FROM base
    WHERE sale_date >= DATE '2023-10-05'
      AND units BETWEEN 25 AND 26
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        rn,
        units_whole_tier,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY revenue DESC NULLS FIRST) revenue_percent_rank,
        RANK() OVER w_ordered revenue_rank,
        RANK() OVER (ORDER BY units_whole DESC) units_whole_rank,
        RANK() OVER (PARTITION BY product_id ORDER BY units_whole) units_whole_rank_2,
        RANK() OVER (PARTITION BY DATE_TRUNC('month', sale_date), product_id ORDER BY units_whole) units_whole_rank_3
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY product_id ORDER BY revenue DESC NULLS LAST)
),
with_dims AS (
    -- Attach products attributes
    SELECT
        s.product_id,
        s.units_whole_tier,
        s.revenue_rank,
        s.units_whole_rank_2,
        p.brand,
        p.launch_date
    FROM metrics s
    INNER JOIN products p
        ON s.product_id = p.id
)
-- Window metrics over several different partitions
SELECT
    product_id,
    revenue_rank,
    units_whole_rank_2,
    DENSE_RANK() OVER (ORDER BY units_whole_rank_2 DESC NULLS FIRST) units_whole_rank_2_dense_rank
FROM with_dims
WHERE product_id IN (4, 10);
