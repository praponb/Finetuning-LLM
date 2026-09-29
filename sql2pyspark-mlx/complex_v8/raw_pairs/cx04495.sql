WITH scoped_claims AS (
    -- Filter claims
    SELECT *
    FROM claims
    WHERE approved_amount <= 34401.0
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        p.status AS policy_status,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM scoped_claims s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(adjuster_level / NULLIF(SUM(adjuster_level) OVER w_policy_status, 0), 4) AS adjuster_level_share_of_policy_status,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(claim_date), adjuster_id ORDER BY claim_date DESC), 3) AS claim_date_cume_dist,
        ROW_NUMBER() OVER (ORDER BY adjuster_level DESC, id) AS seq_no,
        ROUND(CUME_DIST() OVER w_adjuster_level_claim_type_by, 3) AS claim_date_cume_dist_2,
        DENSE_RANK() OVER w_all_by_adjuster_level_ties AS adjuster_level_dense_rank,
        RANK() OVER w_all_by_adjuster_level_ties_2 AS adjuster_level_rank,
        FIRST_VALUE(approved_amount) OVER (PARTITION BY YEAR(claim_date), adjuster_id ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_approved_amount,
        SUM(adjuster_level) OVER (PARTITION BY policy_id, claim_type ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_adjuster_level,
        ROUND(AVG(adjuster_level) OVER (PARTITION BY policy_id, claim_type ORDER BY approved_amount DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS adjuster_level_avg_2
    FROM joined
    WINDOW w_policy_status AS (PARTITION BY policy_status), w_adjuster_level_claim_type_by AS (PARTITION BY adjuster_level, claim_type ORDER BY claim_date DESC NULLS FIRST), w_all_by_adjuster_level_ties AS (ORDER BY adjuster_level), w_all_by_adjuster_level_ties_2 AS (ORDER BY adjuster_level DESC)
),
scored AS (
    -- Derive helper columns
    SELECT *, ABS(running_adjuster_level - 33) AS running_adjuster_level_distance
    FROM with_trends
    WHERE approved_amount > 0.0
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    policy_status,
    adjuster_level,
    adjuster_name,
    adjuster_level_share_of_policy_status,
    claim_date_cume_dist,
    claim_date_cume_dist_2,
    adjuster_level_rank,
    running_adjuster_level,
    adjuster_level_avg_2
FROM scored
ORDER BY adjuster_level_avg_2
