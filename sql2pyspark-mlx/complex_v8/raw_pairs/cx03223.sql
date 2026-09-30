WITH scoped_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
    FROM claims
    WHERE adjuster_id >= 4
      AND claim_type IN ('fire', 'flood', 'theft')
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        MAX(claimed_amount) OVER (PARTITION BY claim_type) AS claim_type_max_claimed_amount,
        COUNT(*) OVER (PARTITION BY claim_type) AS rows_in_claim_type
    FROM scoped_claims
),
windowed_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY approved_amount DESC) AS approved_amount_dense_rank,
        LAST_VALUE(claimed_amount) OVER (ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount
    FROM windowed
),
policy_id_stats AS (
    -- Max claimed_amount per policy_id
    SELECT policy_id, MAX(claimed_amount) AS max_claimed_amount
    FROM windowed_2
    GROUP BY policy_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_claimed_amount,
        ROUND(s.claimed_amount - g.max_claimed_amount, 2) AS claimed_amount_minus_max
    FROM windowed_2 s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.claimed_amount = g.max_claimed_amount
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        rows_in_claim_type,
        max_claimed_amount,
        claimed_amount_minus_max,
        ROUND(last_claimed_amount - AVG(last_claimed_amount) OVER (PARTITION BY approved_amount_dense_rank), 2) AS last_claimed_amount_vs_approved_amount_dense_ran,
        LEAD(approved_amount, 3) OVER w_claim_type_policy_id_by_clai AS next3_approved_amount,
        FIRST_VALUE(approved_amount_dense_rank) OVER (ORDER BY approved_amount_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_approved_amount_dense_rank,
        RANK() OVER (ORDER BY approved_amount_dense_rank) AS approved_amount_dense_rank_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_type ORDER BY claimed_amount_minus_max DESC NULLS FIRST), 3) AS claimed_amount_minus_max_percent_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY approved_amount_dense_rank), 3) AS approved_amount_dense_rank_percent_rank
    FROM above_average
    WINDOW w_claim_type_policy_id_by_clai AS (PARTITION BY claim_type, policy_id ORDER BY claim_type_max_claimed_amount, id)
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claim_type,
        s.max_claimed_amount,
        s.claimed_amount_minus_max,
        s.last_claimed_amount_vs_approved_amount_dense_ran,
        s.first_approved_amount_dense_rank,
        s.approved_amount_dense_rank_percent_rank,
        c.id AS claim_id,
        c.claim_date AS claim_claim_date,
        c.reported_at AS claim_reported_at,
        c.approved_amount AS claim_approved_amount,
        c.adjuster_id AS claim_adjuster_id
    FROM with_windows s
    JOIN claims c ON c.policy_id = s.policy_id
),
with_details_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claim_type,
        s.max_claimed_amount,
        s.claimed_amount_minus_max,
        s.last_claimed_amount_vs_approved_amount_dense_ran,
        s.first_approved_amount_dense_rank,
        s.approved_amount_dense_rank_percent_rank,
        s.claim_id,
        s.claim_claim_date,
        s.claim_reported_at,
        s.claim_approved_amount,
        s.claim_adjuster_id,
        p.premium AS policy_premium,
        p.status AS policy_status
    FROM detail_rows_claims s
    LEFT JOIN policies p ON p.id = s.policy_id
)
SELECT
    claim_adjuster_id,
    first_approved_amount_dense_rank,
    COALESCE(claim_type, 'ALL') AS claim_type,
    GROUPING_ID() AS gid,
    ROUND(100 * COUNT(CASE WHEN policy_status NOT IN ('CANCELLED', 'EXPIRED') THEN 1 END) / COUNT(*), 1) AS pct_policy_status_not_in_cancell
FROM with_details_policy
GROUP BY CUBE(claim_adjuster_id, first_approved_amount_dense_rank, claim_type)
ORDER BY claim_adjuster_id NULLS LAST, first_approved_amount_dense_rank NULLS LAST, claim_type NULLS LAST
