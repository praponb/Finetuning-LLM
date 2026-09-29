WITH prepared_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        CONCAT_WS('|', status, claim_type) AS status_claim_type_label
    FROM claims
),
with_activity AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM prepared_claims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.adjuster_id >= 5
              AND c.approved_amount BETWEEN 492.6 AND 16404.6
        )
),
latest_per_key AS (
    -- Keep the latest row per claim_type, policy_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status_claim_type_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type, policy_id ORDER BY claimed_amount DESC, id DESC) AS recency_rank
        FROM with_activity
    ) ranked
    WHERE recency_rank = 1
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_adjuster_id_claim_type AS rows_in_adjuster_id_claim_type,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY policy_id, adjuster_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date,
        claimed_amount - LAG(claimed_amount) OVER (PARTITION BY policy_id, adjuster_id ORDER BY claim_date, id) AS claimed_amount_change,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER w_adjuster_id_claim_type, 0), 4) AS approved_amount_share_of_adjuster_id_claim_type,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY adjuster_id, claim_type), 0), 4) AS claimed_amount_share_of_adjuster_id_claim_type
    FROM latest_per_key
    WINDOW w_adjuster_id_claim_type AS (PARTITION BY adjuster_id, claim_type)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', claim_date) AS days_since_claim_date,
        LOWER(REGEXP_REPLACE(TRIM(status_claim_type_label), ' ', '_')) AS status_claim_type_label_slug,
        SUBSTRING(TRIM(claim_type), 1, 3) AS claim_type_prefix
    FROM with_windows
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    claimed_amount,
    status_claim_type_label,
    last_known_claim_date,
    claimed_amount_change,
    approved_amount_share_of_adjuster_id_claim_type,
    claimed_amount_share_of_adjuster_id_claim_type,
    claim_type_prefix,
    ABS(approved_amount - 12305.3) AS approved_amount_distance
FROM labeled
ORDER BY last_known_claim_date
