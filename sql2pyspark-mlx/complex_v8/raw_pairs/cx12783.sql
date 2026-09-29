WITH scoped_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at
    FROM claims
    WHERE claim_type <> 'collision'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        reported_at,
        RANK() OVER w_adjuster_id_policy_id_by_rep AS reported_at_rank,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_id, status)) AS distinct_policy_id_in_adjuster_id_status,
        RANK() OVER (PARTITION BY status, YEAR(reported_at) ORDER BY reported_at NULLS LAST) AS reported_at_rank_2,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY adjuster_id, status), 0), 4) AS claimed_amount_share_of_adjuster_id_status
    FROM scoped_claims
    WINDOW w_adjuster_id_policy_id_by_rep AS (PARTITION BY adjuster_id, policy_id ORDER BY reported_at)
),
with_details_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.reported_at,
        s.reported_at_rank,
        s.distinct_policy_id_in_adjuster_id_status,
        p.holder_id AS policy_holder_id
    FROM sequenced s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.holder_id <= 15
)
SELECT
    distinct_policy_id_in_adjuster_id_status,
    reported_at_rank,
    MAX(claim_date) AS last_claim_date,
    ROUND(SUM(reported_at_rank) / NULLIF(SUM(distinct_policy_id_in_adjuster_id_status), 0), 3) AS reported_at_rank_to_distinct_policy_id_in_adjust
FROM with_details_policy
GROUP BY ROLLUP(distinct_policy_id_in_adjuster_id_status, reported_at_rank)
ORDER BY distinct_policy_id_in_adjuster_id_status NULLS LAST, reported_at_rank NULLS LAST
