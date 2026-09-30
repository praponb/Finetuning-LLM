WITH scoped AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        ROUND(claimed_amount / NULLIF(approved_amount, 0), 3) AS claimed_amount_per_approved_amount,
        DATE_TRUNC('QUARTER', claim_date) AS claim_date_quarter,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled
    FROM claims
    WHERE status = 'IN_REVIEW'
      AND claimed_amount >= 19347.2
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        claimed_amount_per_approved_amount,
        claim_date_quarter,
        CASE WHEN claimed_amount >= 42838.9 THEN 'A' WHEN claimed_amount >= 12726.7 THEN 'B' ELSE 'C' END AS claimed_amount_tier,
        CONCAT(claim_type, '#', CAST(id AS STRING)) AS claim_type_ref,
        LEAST(approved_amount, approved_amount_filled) AS least_approved_amount_approved_amount_filled
    FROM scoped
),
combined_details_adjuster AS (
    -- Join adjusters
    SELECT s.*, a.team AS adjuster_team
    FROM derived s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.team IN ('Health', 'Travel')
),
combined_details_adjuster_2 AS (
    -- Join adjusters
    SELECT
        s.id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.claimed_amount_per_approved_amount,
        s.claim_date_quarter,
        s.claimed_amount_tier,
        s.claim_type_ref,
        s.least_approved_amount_approved_amount_filled,
        s.adjuster_team,
        a.adjuster_name,
        a.team AS adjuster_team_2,
        a.level AS adjuster_level
    FROM combined_details_adjuster s
    JOIN adjusters a ON a.id = s.adjuster_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        claim_type,
        claimed_amount,
        claimed_amount_per_approved_amount,
        claimed_amount_tier,
        claim_type_ref,
        least_approved_amount_approved_amount_filled,
        adjuster_name,
        adjuster_team_2,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY adjuster_team, adjuster_level), 2) AS approved_amount_vs_adjuster_team_adjuster_level_,
        LEAD(claim_date_quarter) OVER (ORDER BY claim_date_quarter DESC NULLS FIRST, id) AS next_claim_date_quarter,
        ROUND(CUME_DIST() OVER w_all_by_claim_date_quarter_ti, 3) AS claim_date_quarter_cume_dist,
        SUM(adjuster_level) OVER (ORDER BY claim_date_quarter DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_adjuster_level
    FROM combined_details_adjuster_2
    WINDOW w_all_by_claim_date_quarter_ti AS (ORDER BY claim_date_quarter DESC NULLS FIRST)
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM windowed
    WHERE claim_date_quarter_cume_dist > (SELECT AVG(claim_date_quarter_cume_dist) FROM windowed)
),
stats AS (
    -- Group-level totals
    SELECT
        adjuster_team_2,
        claim_type,
        COUNT(*) FILTER (WHERE adjuster_team_2 = 'Property') AS n_adjuster_team_2_property,
        ROUND(SUM(claimed_amount_per_approved_amount) / NULLIF(SUM(least_approved_amount_approved_amount_filled), 0), 3) AS claimed_amount_per_approved_amount_to_least_appr,
        COUNT(DISTINCT id) AS distinct_ids
    FROM qualified
    WHERE claimed_amount_per_approved_amount <= 6.5
    GROUP BY adjuster_team_2, claim_type
    HAVING MAX(approved_amount_vs_adjuster_team_adjuster_level_) > -40000.0
)
SELECT
    adjuster_team_2,
    claim_type,
    distinct_ids,
    LENGTH(TRIM(adjuster_team_2)) AS adjuster_team_2_len,
    CONCAT(claim_type, '#', CAST(distinct_ids AS STRING)) AS claim_type_ref
FROM stats
WHERE claimed_amount_per_approved_amount_to_least_appr >= 4.0
ORDER BY adjuster_team_2 NULLS LAST
