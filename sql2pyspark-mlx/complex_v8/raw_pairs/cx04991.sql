WITH scoped_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        UPPER(TRIM(status)) AS status_clean,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM policies
    WHERE status IS NOT NULL
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.end_date,
        s.status_or_unknown,
        s.status_clean,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.status AS claim_status,
        c.claim_type
    FROM scoped_policys s
    JOIN claims c ON c.policy_id = s.id AND c.claim_type NOT IN ('collision', 'fire')
)
-- Subtotals with GROUPING SETS
SELECT
    status_or_unknown,
    claim_status,
    COUNT(*) AS cnt
FROM exploded_detail_claims
GROUP BY GROUPING SETS ((status_or_unknown, claim_status), (status_or_unknown), (claim_status), ())
ORDER BY status_or_unknown NULLS LAST, claim_status NULLS LAST
