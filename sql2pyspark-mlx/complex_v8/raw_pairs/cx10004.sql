WITH valid AS (
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
        DATE_FORMAT(reported_at, 'MMM yyyy') AS reported_at_label
    FROM claims
    WHERE approved_amount BETWEEN 799.5 AND 38909.4
),
claim_type_stats AS (
    -- Max claimed_amount per claim_type
    SELECT claim_type, MAX(claimed_amount) AS max_claimed_amount
    FROM valid
    GROUP BY claim_type
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_claimed_amount,
        ROUND(s.claimed_amount - g.max_claimed_amount, 2) AS claimed_amount_minus_max
    FROM valid s
    JOIN claim_type_stats g ON g.claim_type = s.claim_type
    WHERE s.claimed_amount = g.max_claimed_amount
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.reported_at_label,
        s.claimed_amount_minus_max,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.claim_date AS claim_claim_date
    FROM above_average s
    LEFT JOIN claims c ON c.policy_id = s.policy_id
),
screened AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM exploded_detail s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claim_date >= DATE '2024-06-26'
        )
      AND s.claimed_amount > 4554.8
)
SELECT
    claim_id,
    adjuster_id,
    MIN(claimed_amount_minus_max) AS min_claimed_amount_minus_max,
    ROUND(SUM(approved_amount) / NULLIF(SUM(claimed_amount_minus_max), 0), 3) AS approved_amount_to_claimed_amount_minus_max_rati
FROM screened
GROUP BY ROLLUP(claim_id, adjuster_id)
