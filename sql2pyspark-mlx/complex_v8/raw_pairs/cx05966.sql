WITH base AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM claims
    WHERE (adjuster_id IS NOT NULL OR policy_id >= 20)
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.claim_date) FROM claims c WHERE c.policy_id = s.policy_id) AS first_claim_date,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count,
        (SELECT MIN(c.claimed_amount) FROM claims c WHERE c.policy_id = s.policy_id) AS min_claim_claimed_amount
    FROM base s
),
status_stats AS (
    -- Max claimed_amount per status
    SELECT status, MAX(claimed_amount) AS max_claimed_amount
    FROM with_stats
    GROUP BY status
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_claimed_amount,
        ROUND(s.claimed_amount - g.max_claimed_amount, 2) AS claimed_amount_minus_max
    FROM with_stats s
    JOIN status_stats g ON g.status = s.status
    WHERE s.claimed_amount = g.max_claimed_amount
)
SELECT id, metric, metric_value
FROM (
    SELECT id, approved_amount, max_claimed_amount
    FROM above_average
) src
UNPIVOT (
    metric_value FOR metric IN (approved_amount, max_claimed_amount)
) unp
ORDER BY id, metric
