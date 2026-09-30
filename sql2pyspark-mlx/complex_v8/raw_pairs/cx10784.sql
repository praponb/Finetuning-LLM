WITH valid_claims AS (
    -- Keep valid rows and derive helper columns
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
        CONCAT_WS(': ', status, claim_type) AS status_claim_type_label
    FROM claims
    WHERE approved_amount < 24718.5
),
deduped AS (
    -- Keep the latest row per adjuster_id
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
        status_claim_type_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id ORDER BY claimed_amount DESC, id DESC) AS rn
        FROM valid_claims
    ) ranked
    WHERE rn = 1
),
policy_id_stats AS (
    -- Max claimed_amount per policy_id
    SELECT policy_id, MAX(claimed_amount) AS max_claimed_amount
    FROM deduped
    GROUP BY policy_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_claimed_amount,
        ROUND(s.claimed_amount - g.max_claimed_amount, 2) AS claimed_amount_minus_max
    FROM deduped s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.claimed_amount = g.max_claimed_amount
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, amount
FROM (
    SELECT id, claimed_amount_minus_max, max_claimed_amount, claimed_amount, approved_amount
    FROM above_average
) src
UNPIVOT (
    amount FOR measure IN (claimed_amount_minus_max AS claimed_amount_minus_max_v, max_claimed_amount AS max_claimed_amount_v, claimed_amount AS m_claimed_amount, approved_amount AS approved_amount_v)
) unp
