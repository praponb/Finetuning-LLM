WITH scoped AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        INITCAP(TRIM(claim_type)) AS claim_type_clean,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled,
        LENGTH(TRIM(status)) AS status_len
    FROM claims
    WHERE (claim_date < DATE '2025-03-31' OR claim_type = 'fire')
),
with_details AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.claim_type_clean,
        s.claimed_amount_filled,
        s.status_len,
        a.level AS adjuster_level,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM scoped s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        claim_type_clean,
        claimed_amount_filled,
        status_len,
        adjuster_level,
        adjuster_name,
        adjuster_team,
        LEAD(status_len, 2, 0) OVER (PARTITION BY status_len ORDER BY claim_date DESC, id) AS next2_status_len,
        NTILE(5) OVER w_status_len_by_claim_date AS claim_date_quintile
    FROM with_details
    WINDOW w_status_len_by_claim_date AS (PARTITION BY status_len ORDER BY claim_date DESC, id)
),
with_trends_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        claimed_amount_filled,
        status_len,
        adjuster_name,
        adjuster_team,
        ROUND(claim_date_quintile / NULLIF(SUM(claim_date_quintile) OVER (PARTITION BY adjuster_level, adjuster_team), 0), 4) AS claim_date_quintile_share_of_adjuster_level_adju,
        SUM(adjuster_level) OVER (ORDER BY adjuster_level ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_adjuster_level
    FROM with_trends
)
-- Subtotals with GROUPING SETS
SELECT
    claim_type,
    status,
    status_len,
    COUNT(*) AS n_rows,
    SUM(claim_date_quintile_share_of_adjuster_level_adju) AS total_claim_date_quintile_share_of_adjuster_leve
FROM with_trends_2
GROUP BY GROUPING SETS ((claim_type, status, status_len), (claim_type), (status), ())
ORDER BY claim_type NULLS LAST, status NULLS LAST, status_len NULLS LAST
