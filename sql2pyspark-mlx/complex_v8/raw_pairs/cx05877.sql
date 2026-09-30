WITH recent AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        DATE_TRUNC('YEAR', start_date) AS start_date_year
    FROM policies
    WHERE start_date BETWEEN DATE '2024-08-27' AND DATE '2025-09-24'
),
enriched AS (
    -- Enrich with dimension data
    SELECT s.*, p.risk_score AS holder_risk_score, p.joined_on AS holder_joined_on
    FROM recent s
    JOIN policyholders p ON p.id = s.holder_id
),
enriched_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        s.start_date_year,
        s.holder_risk_score,
        s.holder_joined_on,
        p.holder_name
    FROM enriched s
    JOIN policyholders p ON p.id = s.holder_id
)
SELECT
    id,
    start_date,
    start_date_year,
    holder_risk_score,
    holder_joined_on,
    holder_name,
    INITCAP(TRIM(holder_name)) AS holder_name_clean,
    DATEDIFF(DATE '2025-06-30', start_date_year) AS days_since_start_date_year
FROM enriched_holder
ORDER BY id DESC NULLS FIRST
