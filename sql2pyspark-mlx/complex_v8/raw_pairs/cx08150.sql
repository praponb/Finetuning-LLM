WITH prepared_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LOWER(TRIM(product)) AS product_clean,
        DATE_TRUNC('YEAR', start_date) AS start_date_year,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug
    FROM policies
    WHERE status <> 'CANCELLED'
),
derived AS (
    -- Derive helper columns
    SELECT *, CONCAT(product_slug, '#', CAST(id AS STRING)) AS product_slug_ref
    FROM prepared_policys
)
SELECT *
FROM (
    SELECT status, product, premium
    FROM derived
) src
PIVOT (
    SUM(premium)
    FOR product IN ('HEALTH', 'HOME')
)
ORDER BY status
