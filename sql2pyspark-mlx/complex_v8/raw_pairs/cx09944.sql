WITH valid AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        CONCAT_WS('-', status, claim_type) AS status_claim_type_label,
        reported_at + INTERVAL 30 MINUTES AS reported_at_plus_30m
    FROM claims
    WHERE claimed_amount <= 34401.0
      AND claim_type NOT IN ('flood', 'theft')
),
labeled AS (
    -- Label and score rows
    SELECT *, COALESCE(claimed_amount, 0.0) AS claimed_amount_filled
    FROM valid
),
unique_rows AS (
    -- Keep the earliest row per adjuster_id, status
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        status_claim_type_label,
        reported_at_plus_30m,
        claimed_amount_filled
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id, status ORDER BY approved_amount, id) AS recency_rank
        FROM labeled
    ) ranked
    WHERE recency_rank = 1
),
latest AS (
    -- Keep the latest row per claim_type, policy_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        status_claim_type_label,
        reported_at_plus_30m,
        claimed_amount_filled
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type, policy_id ORDER BY approved_amount DESC, id DESC) AS recency_rank
        FROM unique_rows
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    claimed_amount,
    approved_amount,
    status,
    reported_at,
    status_claim_type_label,
    claimed_amount_filled
FROM latest
WHERE claim_date >= DATE '2024-08-01'
ORDER BY status_claim_type_label DESC, reported_at
