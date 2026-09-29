WITH prepared_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        DATEDIFF(DATE '2025-12-31', claim_date) AS days_since_claim_date,
        SUBSTRING(TRIM(claim_type), 1, 4) AS claim_type_prefix,
        CONCAT_WS('-', status, claim_type) AS status_claim_type_label
    FROM claims
    WHERE claimed_amount BETWEEN 17585.2 AND 44921.3
      AND TO_DATE(reported_at) < DATE '2025-02-18'
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        reported_at,
        days_since_claim_date,
        claim_type_prefix,
        status_claim_type_label,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket
    FROM prepared_claims
)
SELECT
    policy_id,
    status,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(claimed_amount_bucket) AS total_claimed_amount_bucket
FROM derived
GROUP BY GROUPING SETS ((policy_id, status), (policy_id), (status), ())
ORDER BY policy_id NULLS LAST, status NULLS LAST
