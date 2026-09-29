WITH recent_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        ROUND(approved_amount / NULLIF(claimed_amount, 0), 3) AS approved_amount_per_claimed_amount
    FROM claims
    WHERE status IN ('IN_REVIEW', 'REJECTED', 'PAID')
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        FIRST_VALUE(claim_date) OVER (PARTITION BY policy_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date,
        LAST_VALUE(claim_date) OVER (PARTITION BY policy_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_date,
        ROW_NUMBER() OVER w_policy_id_by_claim_date AS seq_no,
        FIRST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY policy_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date
    FROM recent_claims
    WINDOW w_policy_id_by_claim_date AS (PARTITION BY policy_id ORDER BY claim_date, id)
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claimed_amount >= 4868.7
        )
      AND s.claim_type IN ('delay', 'theft')
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.approved_amount < 23487.5
        )
)
-- Final projection
SELECT
    id,
    claim_date,
    claim_type,
    last_claim_date,
    seq_no,
    COALESCE(seq_no, 0) AS seq_no_filled
FROM with_activity
ORDER BY seq_no_filled NULLS LAST
