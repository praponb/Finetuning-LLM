WITH prepared_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status
    FROM claims
    WHERE claimed_amount > 18478.3
      AND policy_id >= 13
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        c.id AS claim_id,
        c.claim_date AS claim_claim_date,
        c.reported_at AS claim_reported_at,
        c.approved_amount AS claim_approved_amount,
        c.claim_type AS claim_claim_type
    FROM prepared_claims s
    JOIN claims c ON c.policy_id = s.policy_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        claim_id,
        claim_reported_at,
        claim_approved_amount,
        NTILE(10) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_reported_at) ORDER BY claim_claim_date ASC, id, claim_id) AS claim_claim_date_decile,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY adjuster_id), 2) AS approved_amount_vs_adjuster_id_avg
    FROM line_level
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claimed_amount >= 19609.8
              AND c.status IS NOT NULL
        )
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY policy_id, claim_claim_date_decile)) AS distinct_adjuster_id_in_policy_id_claim_claim_da,
        SUM(approved_amount) OVER (PARTITION BY policy_id, claim_claim_date_decile) AS policy_id_claim_claim_date_decile_sum_approved_a
    FROM without_matches
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    approved_amount,
    claim_id,
    claim_approved_amount,
    claim_claim_date_decile,
    approved_amount_vs_adjuster_id_avg,
    policy_id_claim_claim_date_decile_sum_approved_a,
    DATEDIFF(DATE '2025-12-31', claim_date) AS days_since_claim_date
FROM sequenced
ORDER BY policy_id DESC
