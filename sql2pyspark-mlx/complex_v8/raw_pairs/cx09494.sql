WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        DATE_TRUNC('QUARTER', reported_at) AS reported_at_quarter,
        QUARTER(reported_at) AS reported_at_quarter_2
    FROM claims
    WHERE approved_amount >= 0.0
      AND status NOT IN ('IN_REVIEW', 'APPROVED')
),
with_details_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at_quarter,
        s.reported_at_quarter_2,
        a.level AS adjuster_level,
        a.team AS adjuster_team,
        a.adjuster_name
    FROM cleaned s
    JOIN adjusters a ON a.id = s.adjuster_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.claimed_amount) FROM claims c WHERE c.policy_id = s.policy_id) AS max_claim_claimed_amount,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count
    FROM with_details_adjuster s
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_id,
        claim_type,
        status,
        reported_at_quarter,
        reported_at_quarter_2,
        adjuster_level,
        adjuster_team,
        adjuster_name,
        claim_count,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY policy_id)) AS distinct_status_in_policy_id,
        ROUND(claim_count - AVG(claim_count) OVER (PARTITION BY policy_id), 2) AS claim_count_vs_policy_id_avg,
        DENSE_RANK() OVER (ORDER BY reported_at_quarter DESC, id) AS reported_at_quarter_dense_rank,
        LAST_VALUE(approved_amount) OVER (ORDER BY reported_at_quarter DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_approved_amount,
        LEAD(adjuster_level, 3) OVER (ORDER BY reported_at_quarter DESC, id) AS next3_adjuster_level
    FROM with_stats
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT adjuster_id, claim_type, claim_count
    FROM sequenced
) src
PIVOT (
    SUM(claim_count)
    FOR claim_type IN ('collision', 'medical')
)
