WITH recent AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
enriched_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.reported_at,
        a.team AS adjuster_team
    FROM recent s
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE a.team IS NOT NULL
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        adjuster_team,
        RANK() OVER (PARTITION BY adjuster_team, adjuster_id ORDER BY claimed_amount DESC, id) AS claimed_amount_rank,
        RANK() OVER (PARTITION BY adjuster_team, adjuster_id ORDER BY claimed_amount DESC) AS claimed_amount_rank_2,
        RANK() OVER (PARTITION BY claim_type, adjuster_id ORDER BY claimed_amount ASC) AS claimed_amount_rank_3,
        NTILE(4) OVER w_adjuster_team_adjuster_id_by AS claimed_amount_quartile,
        COUNT(*) OVER w_reported_at_month AS rows_in_reported_at_month,
        MAX(claimed_amount) OVER (PARTITION BY adjuster_team, adjuster_id ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_claimed_amount_so_far
    FROM enriched_adjuster
    WINDOW w_adjuster_team_adjuster_id_by AS (PARTITION BY adjuster_team, adjuster_id ORDER BY claimed_amount DESC, id), w_reported_at_month AS (PARTITION BY MONTH(reported_at))
)
SELECT
    adjuster_team,
    claimed_amount_rank_2,
    claim_type,
    GROUPING_ID() AS gid,
    ROUND(100 * COUNT(CASE WHEN adjuster_team IN ('Health', 'Property', 'Motor') THEN 1 END) / COUNT(*), 1) AS pct_adjuster_team_in_health_prop,
    ARRAY_SORT(COLLECT_SET(adjuster_team)) AS adjuster_team_list
FROM metrics
GROUP BY CUBE(adjuster_team, claimed_amount_rank_2, claim_type)
ORDER BY adjuster_team NULLS LAST, claimed_amount_rank_2 NULLS LAST, claim_type NULLS LAST
