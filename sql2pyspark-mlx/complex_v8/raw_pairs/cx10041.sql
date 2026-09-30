WITH filtered_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        status
    FROM policies
    WHERE (holder_id >= 9 OR status IN ('ACTIVE', 'LAPSED'))
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM filtered_policys
)
-- Subtotals with CUBE
SELECT
    holder_id,
    COALESCE(product, 'ALL') AS product,
    PERCENTILE_APPROX(premium, 0.5) AS median_premium,
    ROUND(SUM(premium), 2) AS total_premium,
    COUNT(*) AS n_rows
FROM labeled
GROUP BY CUBE(holder_id, product)
ORDER BY holder_id NULLS LAST, product NULLS LAST
