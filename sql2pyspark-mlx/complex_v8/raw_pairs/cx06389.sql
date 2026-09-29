WITH recent AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATEDIFF(DATE '2026-01-01', start_date) AS days_since_start_date,
        LOWER(TRIM(status)) AS status_clean
    FROM policies
    WHERE status IS NOT NULL
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_type IN ('delay', 'fire')
              AND c.adjuster_id <= 5
        )
),
enriched_holder AS (
    -- Enrich with dimension data
    SELECT s.*, p.risk_score AS holder_risk_score
    FROM screened s
    JOIN policyholders p ON p.id = s.holder_id
),
combined_details_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status,
        s.days_since_start_date,
        s.holder_risk_score,
        p.holder_name,
        p.region AS holder_region
    FROM enriched_holder s
    JOIN policyholders p ON p.id = s.holder_id
),
deduped AS (
    -- Keep the latest row per status
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        days_since_start_date,
        holder_risk_score,
        holder_name,
        holder_region
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY days_since_start_date DESC, id DESC) AS rn
        FROM combined_details_holder
    ) ranked
    WHERE rn = 1
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        holder_id,
        start_date,
        days_since_start_date,
        holder_name,
        holder_region,
        INITCAP(TRIM(holder_region)) AS holder_region_clean,
        INITCAP(TRIM(holder_name)) AS holder_name_clean,
        CASE WHEN days_since_start_date >= 385 THEN 'HIGH' WHEN days_since_start_date >= 210 THEN 'MEDIUM' ELSE 'LOW' END AS days_since_start_date_tier
    FROM deduped
    WHERE holder_id IS NOT NULL
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.days_since_start_date,
        s.holder_name,
        s.holder_name_clean,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.approved_amount AS claim_approved_amount
    FROM derived s
    LEFT JOIN claims c ON c.policy_id = s.id AND c.approved_amount > 4868.7
)
-- Final projection
SELECT
    id,
    holder_id,
    start_date,
    holder_name,
    holder_name_clean,
    claim_id,
    claim_reported_at,
    claim_approved_amount,
    claim_reported_at + INTERVAL 1 DAYS AS claim_reported_at_plus_1d,
    DATE_TRUNC('QUARTER', claim_reported_at) AS claim_reported_at_quarter
FROM line_level_claims
ORDER BY claim_id
