WITH prepared AS (
    -- Scope the adjusters rows
    SELECT *
    FROM adjusters
    WHERE UPPER(TRIM(adjuster_name)) LIKE 'E%'
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.id
              AND c.claim_date < DATE '2024-09-08'
        )
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.adjuster_name,
        c.id AS claim_id,
        c.claim_type,
        c.approved_amount AS claim_approved_amount
    FROM with_activity s
    JOIN claims c ON c.adjuster_id = s.id
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.adjuster_name,
        s.claim_id,
        s.claim_approved_amount,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_type AS claim_claim_type,
        c.status AS claim_status
    FROM line_level_claims s
    LEFT JOIN claims c ON c.id = s.claim_id
)
SELECT
    claim_id,
    claim_claim_type,
    claim_adjuster_id,
    SUM(claim_approved_amount) AS total_claim_approved_amount
FROM enriched
GROUP BY CUBE(claim_id, claim_claim_type, claim_adjuster_id)
