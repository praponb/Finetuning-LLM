WITH cleaned_adjusters AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE adjuster_name IS NOT NULL
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.team,
        s.level,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.claim_type,
        c.reported_at AS claim_reported_at,
        c.status AS claim_status
    FROM cleaned_adjusters s
    LEFT JOIN claims c ON c.adjuster_id = s.id
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        c.reported_at AS claim_reported_at_2,
        c.claim_type AS claim_claim_type,
        c.claimed_amount AS claim_claimed_amount_2,
        p.end_date AS policy_end_date,
        p.premium AS policy_premium
    FROM line_level s
    JOIN claims c ON c.id = s.claim_id AND c.status IS NOT NULL
    LEFT JOIN policies p ON p.id = c.policy_id
    WHERE c.reported_at >= TIMESTAMP '2025-02-12 07:00:00'
),
claim_type_stats AS (
    -- Max level per claim_type
    SELECT claim_type, MAX(level) AS max_level
    FROM enriched
    GROUP BY claim_type
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_level, ROUND(s.level - g.max_level, 2) AS level_minus_max
    FROM enriched s
    JOIN claim_type_stats g ON g.claim_type = s.claim_type
    WHERE s.level = g.max_level
)
SELECT
    claim_id,
    claim_type,
    claim_status,
    GROUPING_ID() AS gid,
    ROUND(AVG(max_level), 2) AS avg_max_level,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT id) AS distinct_ids
FROM vs_group
GROUP BY GROUPING SETS ((claim_id, claim_type, claim_status), (claim_id), (claim_type), ())
ORDER BY claim_id NULLS LAST, claim_type NULLS LAST, claim_status NULLS LAST
