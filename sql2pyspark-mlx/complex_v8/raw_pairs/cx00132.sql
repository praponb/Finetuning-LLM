WITH base_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE holder_id IS NOT NULL
      AND status = 'CANCELLED'
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_date
    FROM base_policys s
    LEFT JOIN claims c ON c.policy_id = s.id AND c.adjuster_id IS NOT NULL
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        QUARTER(start_date) AS start_date_quarter,
        DATEDIFF(DATE '2026-01-01', claim_date) AS days_since_claim_date
    FROM line_level
    WHERE claim_id IS NOT NULL
)
-- Subtotals with GROUPING SETS
SELECT
    claim_adjuster_id,
    start_date_quarter,
    GROUPING_ID() AS grouping_level,
    MIN(claim_date) AS first_claim_date,
    MIN(start_date) AS first_start_date
FROM labeled
GROUP BY GROUPING SETS ((claim_adjuster_id, start_date_quarter), (claim_adjuster_id), ())
ORDER BY claim_adjuster_id NULLS LAST, start_date_quarter NULLS LAST
