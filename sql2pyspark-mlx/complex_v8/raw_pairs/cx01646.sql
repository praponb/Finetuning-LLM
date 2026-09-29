WITH scoped AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        CONCAT_WS(' / ', claim_type, status) AS claim_type_status_label,
        LEAST(approved_amount, claimed_amount) AS least_approved_amount_claimed_amount
    FROM claims
    WHERE adjuster_id >= 6
),
enriched AS (
    -- Add calculated columns
    SELECT *, DATE_ADD(claim_date, 365) AS claim_date_plus_365d
    FROM scoped
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(approved_amount) OVER (PARTITION BY status, policy_id ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_approved_amount,
        COUNT(*) OVER (PARTITION BY status, policy_id ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY policy_id, status), 0), 4) AS approved_amount_share_of_policy_id_status,
        LAST_VALUE(claim_date_plus_365d) OVER (PARTITION BY status, policy_id ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_date_plus_365d
    FROM enriched
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM with_windows
    WHERE running_count < (SELECT MAX(running_count) FROM with_windows)
),
with_parent_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.least_approved_amount_claimed_amount,
        s.claim_date_plus_365d,
        s.running_approved_amount,
        s.last_claim_date_plus_365d,
        p.holder_id AS policy_holder_id,
        p.status AS policy_status,
        p.premium AS policy_premium
    FROM without_matches s
    JOIN policies p ON p.id = s.policy_id
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, MIN(reported_at) AS first_claim_reported_at
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.first_claim_reported_at
    FROM with_parent_policy s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        s.reported_at,
        s.least_approved_amount_claimed_amount,
        s.running_approved_amount,
        s.last_claim_date_plus_365d,
        s.policy_holder_id,
        s.policy_premium,
        s.first_claim_reported_at,
        c.id AS claim_id,
        c.claim_date,
        c.approved_amount AS claim_approved_amount,
        c.reported_at AS claim_reported_at,
        c.adjuster_id AS claim_adjuster_id
    FROM with_counts s
    JOIN claims c ON c.policy_id = s.policy_id AND c.approved_amount < 32511.8
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    approved_amount,
    policy_holder_id,
    policy_premium,
    claim_date,
    claim_approved_amount,
    claim_reported_at,
    claim_adjuster_id,
    policy_premium - approved_amount AS policy_premium_minus_approved_amount,
    CONCAT(claim_type, '#', CAST(adjuster_id AS STRING)) AS claim_type_ref
FROM line_level
ORDER BY claim_adjuster_id DESC, policy_holder_id DESC
