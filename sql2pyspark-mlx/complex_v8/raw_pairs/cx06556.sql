WITH scoped_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status
    FROM claims
    WHERE claimed_amount > 3703.0
      AND reported_at >= TIMESTAMP '2025-01-25 11:00:00'
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.claimed_amount) FROM claims c WHERE c.policy_id = s.policy_id) AS min_claim_claimed_amount,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.policy_id = s.policy_id) AS last_claim_reported_at
    FROM scoped_claims s
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY status, claim_count ORDER BY claim_date DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_4,
        ROUND(AVG(claim_count) OVER (PARTITION BY claim_type ORDER BY claim_count, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS claim_count_avg_6_prior,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY policy_id), 2) AS claimed_amount_vs_policy_id_avg,
        SIZE(COLLECT_SET(status) OVER w_policy_id) AS distinct_status_in_policy_id
    FROM summarized_rows
    WINDOW w_policy_id AS (PARTITION BY policy_id)
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count_2,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count_3,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count_4
    FROM windowed s
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.claim_count,
        s.last_claim_reported_at,
        s.claimed_amount_avg_4,
        s.claim_count_avg_6_prior,
        s.claim_count_2,
        s.claim_count_3,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.claim_type AS claim_claim_type
    FROM with_stats s
    JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.claim_type NOT IN ('fire', 'delay')
),
latest AS (
    -- Keep the earliest row per policy_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        claim_count,
        last_claim_reported_at,
        claimed_amount_avg_4,
        claim_count_avg_6_prior,
        claim_count_2,
        claim_count_3,
        claim_id,
        claim_reported_at,
        claim_claim_type
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY claim_count_3, id, claim_id) AS rn
        FROM exploded_detail
    ) ranked
    WHERE rn = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT adjuster_id, claim_count, claim_type, claim_count_3
    FROM latest
) src
PIVOT (
    AVG(claim_count_3)
    FOR claim_type IN ('collision', 'theft', 'fire')
)
