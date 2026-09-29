WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, YEAR(opened_date) AS opened_date_year
    FROM stores
    WHERE opened_date >= DATE '2023-08-04'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        opened_date,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date, id) AS row_seq,
        AVG(opened_date_year) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS opened_date_year_avg_5
    FROM filtered
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND YEAR(c.sale_date) >= 2024
        )
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.revenue,
        latest.sale_date
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.revenue, c.sale_date
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(DATEDIFF(opened_date, sale_date) / 30, 1) AS months_sale_date_to_opened_date
    FROM with_latest
    WHERE opened_date_year_avg_5 <> 48
)
SELECT
    id,
    opened_date,
    row_seq,
    opened_date_year_avg_5,
    revenue
FROM valid
ORDER BY id NULLS FIRST, opened_date_year_avg_5
