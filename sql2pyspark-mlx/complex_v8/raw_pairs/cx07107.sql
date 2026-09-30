WITH recent AS (
    -- Filter policies
    SELECT *
    FROM policies
    WHERE start_date BETWEEN DATE '2024-04-03' AND DATE '2025-10-13'
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        c.id AS claim_id,
        c.claim_type,
        c.adjuster_id AS claim_adjuster_id
    FROM recent s
    LEFT JOIN claims c ON c.policy_id = s.id
),
with_parent AS (
    -- Join claims
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.status,
        s.claim_id,
        s.claim_type,
        s.claim_adjuster_id,
        c.status AS claim_status
    FROM exploded_detail_claims s
    JOIN claims c ON c.id = s.claim_id
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        holder_id,
        end_date,
        status,
        claim_id,
        claim_type,
        claim_status,
        DATE_ADD(end_date, 30) AS end_date_plus_30d
    FROM with_parent
)
-- Final projection
SELECT
    id,
    status,
    claim_type,
    end_date_plus_30d,
    CONCAT(status, '#', CAST(holder_id AS STRING)) AS status_ref
FROM annotated
WHERE claim_id IS NOT NULL
ORDER BY end_date_plus_30d DESC
