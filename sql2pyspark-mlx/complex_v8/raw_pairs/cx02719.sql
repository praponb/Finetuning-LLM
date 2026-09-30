WITH valid_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        ROUND(approved_amount / NULLIF(claimed_amount, 0), 3) AS approved_amount_per_claimed_amount,
        CASE WHEN approved_amount >= 22250.6 THEN 'HIGH' WHEN approved_amount >= 2349.3 THEN 'MEDIUM' ELSE 'LOW' END AS approved_amount_tier
    FROM claims
    WHERE approved_amount > 5336.7
),
joined_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.status,
        s.approved_amount_per_claimed_amount,
        a.team AS adjuster_team
    FROM valid_claims s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.level >= 2
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        adjuster_id,
        claim_type,
        status,
        approved_amount_per_claimed_amount,
        adjuster_team,
        ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY approved_amount_per_claimed_amount DESC, id) AS approved_amount_per_claimed_amount_seq,
        RANK() OVER w_policy_id_by_approved_amount_2 AS approved_amount_per_claimed_amount_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status, claim_type ORDER BY approved_amount_per_claimed_amount), 3) AS approved_amount_per_claimed_amount_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY status, claim_type ORDER BY approved_amount_per_claimed_amount, id) AS approved_amount_per_claimed_amount_seq_2,
        ROW_NUMBER() OVER (PARTITION BY adjuster_team ORDER BY approved_amount_per_claimed_amount, id) AS rn,
        DENSE_RANK() OVER (PARTITION BY status, claim_type ORDER BY approved_amount_per_claimed_amount) AS approved_amount_per_claimed_amount_dense_rank,
        NTILE(5) OVER w_adjuster_id_adjuster_team_by AS approved_amount_per_claimed_amount_quintile,
        ROUND(CUME_DIST() OVER w_adjuster_team_by_approved_am_2, 3) AS approved_amount_per_claimed_amount_cume_dist,
        ROW_NUMBER() OVER w_policy_id_by_approved_amount_3 AS approved_amount_per_claimed_amount_seq_3,
        RANK() OVER (PARTITION BY adjuster_id, adjuster_team ORDER BY approved_amount_per_claimed_amount ASC, id) AS approved_amount_per_claimed_amount_rank_2,
        ROUND(CUME_DIST() OVER (PARTITION BY policy_id ORDER BY approved_amount_per_claimed_amount), 3) AS approved_amount_per_claimed_amount_cume_dist_2,
        DENSE_RANK() OVER (PARTITION BY policy_id ORDER BY approved_amount_per_claimed_amount) AS approved_amount_per_claimed_amount_dense_rank_2
    FROM joined_adjuster
    WINDOW w_policy_id_by_approved_amount_2 AS (PARTITION BY policy_id ORDER BY approved_amount_per_claimed_amount), w_adjuster_id_adjuster_team_by AS (PARTITION BY adjuster_id, adjuster_team ORDER BY approved_amount_per_claimed_amount ASC, id), w_adjuster_team_by_approved_am_2 AS (PARTITION BY adjuster_team ORDER BY approved_amount_per_claimed_amount), w_policy_id_by_approved_amount_3 AS (PARTITION BY policy_id ORDER BY approved_amount_per_claimed_amount, id)
)
SELECT
    id,
    adjuster_id,
    claim_type,
    status,
    approved_amount_per_claimed_amount,
    adjuster_team,
    approved_amount_per_claimed_amount_seq,
    approved_amount_per_claimed_amount_rank,
    approved_amount_per_claimed_amount_seq_2,
    rn,
    approved_amount_per_claimed_amount_seq_3,
    approved_amount_per_claimed_amount_rank_2,
    approved_amount_per_claimed_amount_cume_dist_2,
    approved_amount_per_claimed_amount_dense_rank_2,
    ABS(rn - 5) AS rn_distance,
    FLOOR(rn / 2) * 2 AS rn_bucket
FROM windowed
WHERE status IS NOT NULL
ORDER BY adjuster_team
