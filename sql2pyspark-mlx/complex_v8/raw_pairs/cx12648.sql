WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 7 AND 11
      AND adjuster_id IS NOT NULL
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) AS claim_count,
        ROUND(AVG(approved_amount), 2) AS avg_claim_approved_amount,
        COUNT(*) AS claim_count_2
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        c.avg_claim_approved_amount,
        COALESCE(c.claim_count_2, 0) AS claim_count_2
    FROM valid s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
)
-- Subtotals with GROUPING SETS
SELECT
    claim_type,
    claim_count_2,
    GROUPING(claim_type) AS is_total_claim_type,
    SUM(CASE WHEN status NOT IN ('IN_REVIEW', 'APPROVED') THEN claimed_amount ELSE 0 END) AS claimed_amount_status_not_in_in_review_appr,
    ROUND(AVG(avg_claim_approved_amount), 2) AS avg_avg_claim_approved_amount,
    MIN(claim_count_2) AS min_claim_count_2
FROM summarized_rows
GROUP BY GROUPING SETS ((claim_type, claim_count_2), (claim_type), ())
