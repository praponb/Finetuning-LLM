WITH base_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        ROUND(premium * 1.07, 2) AS premium_adj,
        ROUND(premium * 0.9, 2) AS premium_adj_2
    FROM policies
    WHERE premium >= 1042.5
      AND YEAR(end_date) = 2025
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        premium_adj,
        premium_adj_2,
        DATE_TRUNC('MONTH', start_date) AS start_date_month
    FROM base_policys
    WHERE product = 'AUTO'
)
-- Subtotals with GROUPING SETS
SELECT
    holder_id,
    status,
    ROUND(AVG(premium), 2) AS avg_premium
FROM annotated
GROUP BY GROUPING SETS ((holder_id, status), (holder_id), ())
