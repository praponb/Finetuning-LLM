WITH cleaned_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        SUBSTRING(TRIM(product), 1, 4) AS product_prefix
    FROM policies
    WHERE end_date BETWEEN DATE '2025-10-12' AND DATE '2026-09-23'
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix,
        DATEDIFF(DATE '2025-12-31', start_date) AS days_since_start_date
    FROM cleaned_policys
),
with_details_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status,
        s.product_prefix,
        s.days_since_start_date,
        p.segment AS holder_segment,
        p.holder_name,
        p.region AS holder_region
    FROM labeled s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
deduped AS (
    -- Keep the latest row per holder_region, holder_segment
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        product_prefix,
        days_since_start_date,
        holder_segment,
        holder_name,
        holder_region
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_region, holder_segment ORDER BY days_since_start_date DESC, id DESC) AS recency_rank
        FROM with_details_holder
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    start_date,
    end_date,
    status,
    product_prefix,
    days_since_start_date,
    holder_name,
    holder_region,
    DATE_TRUNC('YEAR', end_date) AS end_date_year,
    DATE_TRUNC('WEEK', start_date) AS start_date_week
FROM deduped
WHERE product_prefix IS NOT NULL
ORDER BY end_date, id
LIMIT 10
