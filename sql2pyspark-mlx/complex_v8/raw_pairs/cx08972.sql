WITH prepared_adjusters AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        CONCAT_WS(' / ', adjuster_name, team) AS adjuster_name_team_label,
        CASE WHEN level >= 3 THEN 'large' WHEN level >= 2 THEN 'medium' ELSE 'small' END AS level_tier
    FROM adjusters
    WHERE level < 3
      AND LOWER(adjuster_name) LIKE '% s%'
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.id) AS claim_count,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.adjuster_id = s.id) AS first_claim_reported_at,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.adjuster_id = s.id) AS first_claim_reported_at_2
    FROM prepared_adjusters s
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        YEAR(first_claim_reported_at) AS first_claim_reported_at_year,
        TO_DATE(first_claim_reported_at) AS first_claim_reported_at_date
    FROM with_counts
    WHERE claim_count < 7
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.id
              AND c.claim_type = 'delay'
        )
),
with_activity_2 AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_activity s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.id
        )
),
screened AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM with_activity_2 s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.id
              AND c.claim_date < DATE '2025-06-24'
              AND c.claim_type = 'delay'
        )
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN level_tier IN ('large', 'medium') THEN 'group_a' WHEN level_tier = 'small' THEN 'group_b' ELSE 'other' END AS level_tier_group,
        LENGTH(TRIM(level_tier)) AS level_tier_len
    FROM screened
)
-- Final projection
SELECT
    id,
    adjuster_name,
    team,
    adjuster_name_team_label,
    level_tier,
    claim_count,
    first_claim_reported_at,
    first_claim_reported_at_year,
    first_claim_reported_at_date,
    level_tier_len,
    ABS(level - 3) AS level_distance,
    first_claim_reported_at + INTERVAL 2 HOURS AS first_claim_reported_at_plus_2h
FROM scored
ORDER BY adjuster_name DESC, first_claim_reported_at_year NULLS LAST, id
LIMIT 20
