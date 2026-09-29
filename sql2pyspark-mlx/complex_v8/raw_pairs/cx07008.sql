WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        DATE_TRUNC('MONTH', end_date) AS end_date_month
    FROM policies
    WHERE product <> 'HEALTH'
),
rollup_base AS (
    -- Group-level totals
    SELECT
        YEAR(end_date_month) AS end_date_month_year,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_ym,
        product,
        COUNT(*) AS row_count,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids
    FROM base
    GROUP BY YEAR(end_date_month), DATE_FORMAT(end_date, 'yyyy-MM'), product
),
annotated AS (
    -- Label and score rows
    SELECT
        end_date_month_year,
        end_date_ym,
        product,
        distinct_holder_ids,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown
    FROM rollup_base
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT distinct_holder_ids, product, end_date_month_year
    FROM annotated
) src
PIVOT (
    SUM(end_date_month_year)
    FOR product IN ('HOME', 'AUTO', 'TRAVEL')
)
ORDER BY distinct_holder_ids
