WITH prepared AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        LENGTH(TRIM(status)) AS status_len,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix,
        CONCAT(claim_type, '#', CAST(adjuster_id AS STRING)) AS claim_type_ref
    FROM claims
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM prepared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.reported_at >= TIMESTAMP '2025-02-06 15:00:00'
        )
      AND s.policy_id IS NOT NULL
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count,
        (SELECT SUM(c.claimed_amount) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS total_claim_claimed_amount,
        (SELECT MIN(c.claimed_amount) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS min_claim_claimed_amount
    FROM filtered s
),
latest_per_key AS (
    -- Keep the latest row per status, policy_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        status_len,
        status_prefix,
        claim_type_ref,
        claim_count,
        total_claim_claimed_amount,
        min_claim_claimed_amount
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status, policy_id ORDER BY claimed_amount DESC, id DESC) AS row_num
        FROM with_counts
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claimed_amount,
    status,
    status_len,
    status_prefix,
    claim_type_ref,
    min_claim_claimed_amount,
    DATE_FORMAT(claim_date, 'yyyy-MM-dd') AS claim_date_label
FROM latest_per_key
ORDER BY claimed_amount
