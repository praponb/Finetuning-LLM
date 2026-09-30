WITH cleaned_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        reported_at + INTERVAL 2 HOURS AS reported_at_plus_2h,
        GREATEST(claimed_amount, approved_amount) AS greatest_claimed_amount_approved_amount
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
with_parent_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.team AS adjuster_team,
        a.level AS adjuster_level,
        p.status AS policy_status
    FROM cleaned_claims s
    JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        reported_at_plus_2h,
        greatest_claimed_amount_approved_amount,
        adjuster_team,
        ROW_NUMBER() OVER (PARTITION BY claim_type, policy_id ORDER BY reported_at_plus_2h, id) AS rn,
        ROUND(AVG(claimed_amount) OVER w_adjuster_id, 2) AS adjuster_id_avg_claimed_amount,
        COUNT(*) OVER (PARTITION BY adjuster_id) AS rows_in_adjuster_id,
        MIN(approved_amount) OVER w_adjuster_id AS adjuster_id_min_approved_amount,
        NTH_VALUE(adjuster_level, 2) OVER (PARTITION BY claim_type, policy_id ORDER BY reported_at_plus_2h, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_adjuster_level
    FROM with_parent_adjuster
    WINDOW w_adjuster_id AS (PARTITION BY adjuster_id)
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        COUNT(*) FILTER (WHERE claim_type NOT IN ('collision', 'medical')) AS claim_claim_type_count
    FROM claims
    GROUP BY adjuster_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count
    FROM with_windows s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
)
SELECT
    COALESCE(adjuster_team, 'ALL') AS adjuster_team,
    second_adjuster_level,
    ROUND(SUM(claim_claim_type_count) / NULLIF(SUM(rn), 0), 3) AS claim_claim_type_count_to_rn_ratio,
    MAX(second_adjuster_level) AS max_second_adjuster_level
FROM summarized_rows
GROUP BY GROUPING SETS ((adjuster_team, second_adjuster_level), (adjuster_team))
ORDER BY adjuster_team NULLS LAST, second_adjuster_level NULLS LAST
