WITH recent_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        CASE WHEN approved_amount >= 23120.7 THEN 'large' WHEN approved_amount >= 0.0 THEN 'medium' ELSE 'small' END AS approved_amount_tier,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref
    FROM claims
    WHERE policy_id <= 28
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        approved_amount_tier,
        ROW_NUMBER() OVER (PARTITION BY claim_type, MONTH(claim_date) ORDER BY claimed_amount, id) AS seq_no,
        approved_amount - LAG(approved_amount) OVER w_claim_type_claim_date_month AS approved_amount_change,
        MIN(claimed_amount) OVER (PARTITION BY claim_type, MONTH(claim_date) ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_claimed_amount_so_far
    FROM recent_claims
    WINDOW w_claim_type_claim_date_month AS (PARTITION BY claim_type, MONTH(claim_date) ORDER BY claimed_amount, id)
),
derived AS (
    -- Add calculated columns
    SELECT *, COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
    FROM with_windows
)
SELECT
    policy_id,
    claim_type_or_unknown,
    seq_no,
    GROUPING(policy_id) AS is_total_policy_id,
    DATEDIFF(MAX(claim_date), MIN(claim_date)) AS claim_date_span_days
FROM derived
GROUP BY GROUPING SETS ((policy_id, claim_type_or_unknown, seq_no), (policy_id), (claim_type_or_unknown), ())
ORDER BY policy_id NULLS LAST, claim_type_or_unknown NULLS LAST, seq_no NULLS LAST
