WITH filtered AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM policies
    WHERE product = 'HEALTH'
      AND holder_id <= 8
),
joined_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.days_start_date_to_end_date,
        p.risk_score AS holder_risk_score,
        p.holder_name
    FROM filtered s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        start_date,
        holder_risk_score,
        holder_name,
        ROUND(holder_risk_score * 1.5, 2) AS holder_risk_score_adj,
        DATE_TRUNC('MONTH', end_date) AS end_date_month
    FROM joined_holder
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(holder_name)) AS holder_name_len,
        CONCAT(holder_name, '#', CAST(id AS STRING)) AS holder_name_ref
    FROM scored
    WHERE holder_name IS NOT NULL
),
enriched_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.holder_risk_score,
        s.holder_name,
        s.end_date_month,
        s.holder_name_len,
        s.holder_name_ref,
        p.region AS holder_region,
        p.holder_name AS holder_holder_name
    FROM annotated s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
without_matches AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM enriched_holder s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND p.risk_score >= 0.2
              AND LOWER(p.holder_name) LIKE '%uw%'
        )
)
SELECT
    id,
    start_date,
    holder_risk_score,
    end_date_month,
    holder_name_len,
    holder_holder_name,
    DATEDIFF(DATE '2025-06-30', end_date_month) AS days_since_end_date_month
FROM without_matches
WHERE start_date >= DATE '2025-01-05'
ORDER BY holder_name_len DESC
