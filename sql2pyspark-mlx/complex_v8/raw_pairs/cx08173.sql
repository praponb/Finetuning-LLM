WITH filtered AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        CONCAT_WS('|', status, product) AS status_product_label,
        CONCAT(status, '#', CAST(holder_id AS STRING)) AS status_ref
    FROM policies
    WHERE holder_id IS NOT NULL
      AND YEAR(end_date) = 2026
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        YEAR(start_date) AS start_date_year,
        CONCAT_WS('|', product, status_ref) AS product_status_ref_label
    FROM filtered
),
derived AS (
    -- Add calculated columns
    SELECT *, TO_DATE(start_date_month) AS start_date_month_date
    FROM tagged
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        start_date_month,
        status_product_label,
        status_ref,
        start_date_year,
        start_date_month_date,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM derived
)
SELECT *
FROM (
    SELECT start_date_month_date, status, premium_bucket
    FROM enriched
) src
PIVOT (
    MAX(premium_bucket)
    FOR status IN ('LAPSED', 'CANCELLED', 'EXPIRED', 'ACTIVE')
)
ORDER BY start_date_month_date
