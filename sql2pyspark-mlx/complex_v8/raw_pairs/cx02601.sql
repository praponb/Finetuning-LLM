WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LENGTH(TRIM(product)) AS product_len,
        DATE_TRUNC('YEAR', start_date) AS start_date_year,
        DATE_FORMAT(end_date, 'MMM yyyy') AS end_date_label
    FROM policies
    WHERE status NOT IN ('ACTIVE', 'CANCELLED')
      AND premium BETWEEN 2096.6 AND 4417.0
),
filtered AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM scoped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND YEAR(p.end_date) = 2026
              AND p.start_date BETWEEN DATE '2024-08-27' AND DATE '2025-05-02'
        )
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        product_len,
        start_date_year,
        end_date_label,
        ROUND(product_len / NULLIF(premium, 0), 3) AS product_len_per_premium
    FROM filtered
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.start_date_year,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.reported_at AS claim_reported_at,
        c.status AS claim_status
    FROM enriched s
    LEFT JOIN claims c ON c.policy_id = s.id
)
SELECT *
FROM (
    SELECT claim_id, claim_status, claim_approved_amount
    FROM detail_rows
) src
PIVOT (
    AVG(claim_approved_amount) AS avg_v, MAX(claim_approved_amount) AS max_v
    FOR claim_status IN ('PAID', 'REJECTED')
)
ORDER BY claim_id
