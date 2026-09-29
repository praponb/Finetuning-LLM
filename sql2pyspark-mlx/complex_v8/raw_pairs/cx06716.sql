WITH prepared AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        reported_at
    FROM claims
    WHERE claimed_amount >= 5774.6
      AND approved_amount BETWEEN 0.0 AND 26237.1
),
with_details AS (
    -- Attach policy attributes
    SELECT s.*, p.status AS policy_status, p.holder_id AS policy_holder_id
    FROM prepared s
    JOIN policies p ON p.id = s.policy_id
),
policy_holder_id_stats AS (
    -- Max claimed_amount per policy_holder_id
    SELECT policy_holder_id, MAX(claimed_amount) AS max_claimed_amount
    FROM with_details
    GROUP BY policy_holder_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_claimed_amount,
        ROUND(s.claimed_amount - g.max_claimed_amount, 2) AS claimed_amount_minus_max
    FROM with_details s
    JOIN policy_holder_id_stats g ON g.policy_holder_id = s.policy_holder_id
    WHERE s.claimed_amount = g.max_claimed_amount
)
SELECT id, field, value
FROM (
    SELECT id, claimed_amount, claimed_amount_minus_max
    FROM above_average
) src
UNPIVOT (
    value FOR field IN (claimed_amount, claimed_amount_minus_max)
) unp
ORDER BY id, field
