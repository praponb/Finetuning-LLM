WITH prepared AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        status,
        DATEDIFF(DATE '2025-12-31', claim_date) AS days_since_claim_date,
        DATE_FORMAT(reported_at, 'yyyy-MM') AS reported_at_label,
        GREATEST(claimed_amount, approved_amount) AS greatest_claimed_amount_approved_amount
    FROM claims
    WHERE claim_date IS NOT NULL
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, SUM(claimed_amount) AS total_claim_claimed_amount
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.total_claim_claimed_amount
    FROM prepared s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
adjuster_id_stats AS (
    -- Max approved_amount per adjuster_id
    SELECT adjuster_id, MAX(approved_amount) AS max_approved_amount
    FROM with_counts
    GROUP BY adjuster_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_approved_amount,
        ROUND(s.approved_amount - g.max_approved_amount, 2) AS approved_amount_minus_max
    FROM with_counts s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.approved_amount = g.max_approved_amount
),
filtered AS (
    -- Keep rows whose policy matches
    SELECT s.*
    FROM above_average s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.id = s.policy_id
              AND p.status = 'LAPSED'
              AND p.product = 'LIFE'
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, reading
FROM (
    SELECT id, approved_amount_minus_max, claimed_amount, greatest_claimed_amount_approved_amount, max_approved_amount
    FROM filtered
) src
UNPIVOT (
    reading FOR field IN (approved_amount_minus_max, claimed_amount, greatest_claimed_amount_approved_amount, max_approved_amount)
) unp
ORDER BY id, field
