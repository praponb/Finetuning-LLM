WITH prepared AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        GREATEST(approved_amount, claimed_amount) AS greatest_approved_amount_claimed_amount
    FROM claims
),
combined_details_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.greatest_approved_amount_claimed_amount,
        a.team AS adjuster_team,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM prepared s
    JOIN adjusters a ON a.id = s.adjuster_id
),
adjuster_id_stats AS (
    -- Max adjuster_level per adjuster_id
    SELECT adjuster_id, MAX(adjuster_level) AS max_adjuster_level
    FROM combined_details_adjuster
    GROUP BY adjuster_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_adjuster_level,
        ROUND(s.adjuster_level - g.max_adjuster_level, 2) AS adjuster_level_minus_max
    FROM combined_details_adjuster s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.adjuster_level = g.max_adjuster_level
),
with_details AS (
    -- Attach adjuster attributes
    SELECT s.*, a.adjuster_name AS adjuster_adjuster_name
    FROM outliers s
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE a.adjuster_name IS NOT NULL
)
SELECT
    adjuster_team,
    policy_id,
    GROUPING_ID() AS grouping_level,
    ROUND(STDDEV(greatest_approved_amount_claimed_amount), 2) AS stddev_greatest_approved_amount_claimed_amount,
    SUM(CASE WHEN UPPER(TRIM(adjuster_name)) LIKE 'F%' THEN approved_amount ELSE 0 END) AS approved_amount_upper_trim_adjuster_name_lik
FROM with_details
GROUP BY GROUPING SETS ((adjuster_team, policy_id), (adjuster_team), ())
ORDER BY adjuster_team NULLS LAST, policy_id NULLS LAST
