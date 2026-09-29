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
        LAST_DAY(claim_date) AS claim_date_month_end
    FROM claims
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.claim_date_month_end,
        p.holder_id AS policy_holder_id,
        p.premium AS policy_premium
    FROM base s
    LEFT JOIN policies p ON p.id = s.policy_id
),
with_details_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.claim_date_month_end,
        s.policy_holder_id,
        p.end_date AS policy_end_date
    FROM with_parent s
    JOIN policies p ON p.id = s.policy_id
),
adjuster_id_stats AS (
    -- Avg approved_amount per adjuster_id
    SELECT adjuster_id, AVG(approved_amount) AS avg_approved_amount
    FROM with_details_policy
    GROUP BY adjuster_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_approved_amount,
        ROUND(s.approved_amount - g.avg_approved_amount, 2) AS approved_amount_minus_avg
    FROM with_details_policy s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.approved_amount > g.avg_approved_amount
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, value
FROM (
    SELECT id, claimed_amount, approved_amount, avg_approved_amount, approved_amount_minus_avg
    FROM outliers
) src
UNPIVOT (
    value FOR metric IN (claimed_amount, approved_amount, avg_approved_amount, approved_amount_minus_avg)
) unp
