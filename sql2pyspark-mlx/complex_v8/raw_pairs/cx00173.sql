WITH filtered AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        CASE WHEN claim_type IN ('flood', 'medical') THEN 'group_a' WHEN claim_type = 'fire' THEN 'group_b' ELSE 'other' END AS claim_type_group
    FROM claims
    WHERE status IS NOT NULL
),
filtered_2 AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.adjuster_id IS NOT NULL
              AND c.claim_date >= DATE '2024-04-09'
        )
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(claim_type_group)) AS claim_type_group_len,
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled
    FROM filtered_2
    WHERE TO_DATE(reported_at) < DATE '2025-03-14'
),
combined_details AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.claim_type_group,
        s.claim_type_group_len,
        a.level AS adjuster_level,
        a.team AS adjuster_team
    FROM annotated s
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE a.level >= 2
),
adjuster_id_stats AS (
    -- Avg approved_amount per adjuster_id
    SELECT adjuster_id, AVG(approved_amount) AS avg_approved_amount
    FROM combined_details
    GROUP BY adjuster_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_approved_amount,
        ROUND(s.approved_amount - g.avg_approved_amount, 2) AS approved_amount_minus_avg
    FROM combined_details s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.approved_amount > g.avg_approved_amount
)
SELECT
    id,
    adjuster_id,
    claim_type,
    status,
    reported_at,
    claim_type_group,
    claim_type_group_len,
    adjuster_level,
    adjuster_team,
    avg_approved_amount,
    approved_amount_minus_avg,
    CONCAT(adjuster_team, '#', CAST(policy_id AS STRING)) AS adjuster_team_ref,
    LOWER(REGEXP_REPLACE(TRIM(claim_type_group), ' ', '_')) AS claim_type_group_slug
FROM compared
ORDER BY adjuster_team NULLS LAST, id DESC NULLS FIRST
