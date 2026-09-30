WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        reported_at,
        LENGTH(TRIM(status)) AS status_len
    FROM claims
    WHERE policy_id >= 25
      AND status <> 'APPROVED'
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.status_len,
        c.id AS claim_id,
        c.claim_date AS claim_claim_date,
        c.claim_type AS claim_claim_type,
        c.status AS claim_status
    FROM filtered s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        DAYOFWEEK(claim_claim_date) AS claim_claim_date_dayofweek,
        DATEDIFF(DATE '2025-06-30', claim_claim_date) AS days_since_claim_claim_date,
        CASE WHEN status_len >= 11 THEN 'large' WHEN status_len >= 6 THEN 'medium' ELSE 'small' END AS status_len_tier
    FROM detail_rows_claims
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.status_len,
        s.claim_id,
        s.claim_claim_type,
        s.claim_claim_date_dayofweek,
        s.days_since_claim_claim_date,
        s.status_len_tier,
        c.id AS claim_id_2,
        c.claim_date,
        c.status AS claim_status_2,
        c.claim_type,
        c.approved_amount AS claim_approved_amount
    FROM annotated s
    LEFT JOIN claims c ON c.policy_id = s.policy_id AND c.claim_type NOT IN ('medical', 'fire')
)
-- Subtotals with GROUPING SETS
SELECT
    claim_id,
    claim_id_2,
    claim_claim_date_dayofweek,
    MIN(status_len) AS min_status_len,
    COUNT(*) AS cnt,
    ARRAY_SORT(COLLECT_SET(claim_status_2)) AS claim_status_2_list
FROM line_level_claims
GROUP BY GROUPING SETS ((claim_id, claim_id_2, claim_claim_date_dayofweek), (claim_id), ())
ORDER BY claim_id NULLS LAST, claim_id_2 NULLS LAST, claim_claim_date_dayofweek NULLS LAST
