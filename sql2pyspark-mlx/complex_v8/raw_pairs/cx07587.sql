WITH prepared_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        LENGTH(TRIM(status)) AS status_len,
        DATE_TRUNC('MONTH', reported_at) AS reported_at_month,
        ROUND(claimed_amount / NULLIF(approved_amount, 0), 3) AS claimed_amount_per_approved_amount
    FROM claims
    WHERE claim_date < DATE '2025-10-29'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        status,
        status_len,
        reported_at_month,
        claimed_amount_per_approved_amount,
        SUM(status_len) OVER (PARTITION BY adjuster_id, policy_id) AS adjuster_id_policy_id_sum_status_len,
        ROUND(AVG(approved_amount) OVER w_adjuster_id_policy_id, 2) AS adjuster_id_policy_id_avg_approved_amount,
        LAST_VALUE(status_len) IGNORE NULLS OVER (PARTITION BY status_len, adjuster_id ORDER BY claimed_amount_per_approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_status_len
    FROM prepared_claims
    WINDOW w_adjuster_id_policy_id AS (PARTITION BY adjuster_id, policy_id)
),
joined AS (
    -- Enrich with dimension data
    SELECT s.*, a.adjuster_name, a.team AS adjuster_team
    FROM sequenced s
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE LOWER(a.adjuster_name) LIKE '%va%'
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        s.reported_at_month,
        s.adjuster_id_policy_id_avg_approved_amount,
        s.adjuster_name,
        s.adjuster_team,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.claim_type,
        c.status AS claim_status,
        c.claim_date
    FROM joined s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.claim_date IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, claim_id, field, metric_value
FROM (
    SELECT id, claim_id, adjuster_id_policy_id_avg_approved_amount, approved_amount
    FROM with_children
) src
UNPIVOT (
    metric_value FOR field IN (adjuster_id_policy_id_avg_approved_amount, approved_amount)
) unp
ORDER BY id, claim_id, field
