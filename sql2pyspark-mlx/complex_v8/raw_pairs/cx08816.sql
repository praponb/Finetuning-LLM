WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
),
rollup_base AS (
    -- Summarise per group
    SELECT product, ROUND(STDDEV(premium), 2) AS stddev_premium, COUNT(*) AS n_rows
    FROM scoped_policys
    GROUP BY product
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        product,
        n_rows,
        ROUND(PERCENT_RANK() OVER w_product_by_n_rows, 3) AS n_rows_percent_rank,
        NTH_VALUE(n_rows, 2) OVER (ORDER BY stddev_premium DESC, product ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_n_rows
    FROM rollup_base
    WINDOW w_product_by_n_rows AS (PARTITION BY product ORDER BY n_rows, product)
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_trends
    WHERE n_rows > (SELECT AVG(n_rows) FROM with_trends)
)
-- Native SQL UNPIVOT (wide to long)
SELECT product, field, value
FROM (
    SELECT product, second_n_rows, n_rows
    FROM with_activity
) src
UNPIVOT (
    value FOR field IN (second_n_rows, n_rows)
) unp
