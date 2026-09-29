WITH filtered_policys AS (
    -- Scope the policies rows
    SELECT *
    FROM policies
    WHERE holder_id IS NOT NULL
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        DATEDIFF(DATE '2025-12-31', end_date) AS days_since_end_date,
        CASE WHEN product IN ('TRAVEL', 'LIFE') THEN 'group_a' WHEN product = 'HOME' THEN 'group_b' ELSE 'other' END AS product_group
    FROM filtered_policys
),
labeled_2 AS (
    -- Add calculated columns
    SELECT
        *,
        DAYOFWEEK(end_date) AS end_date_dayofweek,
        INITCAP(TRIM(product_group)) AS product_group_clean
    FROM labeled
    WHERE status_slug IS NOT NULL
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT end_date_dayofweek, product_group, premium
    FROM labeled_2
) src
PIVOT (
    MIN(premium)
    FOR product_group IN ('group_b', 'other', 'group_a')
)
ORDER BY end_date_dayofweek
