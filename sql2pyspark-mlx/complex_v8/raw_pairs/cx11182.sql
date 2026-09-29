WITH filtered AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        DAYOFWEEK(claim_date) AS claim_date_dayofweek,
        LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
combined_details_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claim_type_slug,
        a.adjuster_name,
        a.team AS adjuster_team,
        a.level AS adjuster_level,
        p.end_date AS policy_end_date
    FROM filtered s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.team IS NOT NULL
    JOIN policies p ON p.id = s.policy_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug_2,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown,
        DATE_FORMAT(policy_end_date, 'yyyy-MM') AS policy_end_date_label
    FROM combined_details_adjuster
    WHERE UPPER(TRIM(adjuster_name)) LIKE 'B%'
),
claim_type_stats AS (
    -- Max adjuster_level per claim_type
    SELECT claim_type, MAX(adjuster_level) AS max_adjuster_level
    FROM enriched
    GROUP BY claim_type
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_adjuster_level,
        ROUND(s.adjuster_level - g.max_adjuster_level, 2) AS adjuster_level_minus_max
    FROM enriched s
    JOIN claim_type_stats g ON g.claim_type = s.claim_type
    WHERE s.adjuster_level = g.max_adjuster_level
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE c.claim_type = 'collision') FROM claims c WHERE c.policy_id = s.policy_id) AS claim_claim_type_count
    FROM above_average s
),
combined_details AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claim_type_slug,
        s.adjuster_team,
        s.adjuster_level,
        s.policy_end_date,
        s.claim_type_slug_2,
        s.policy_end_date_label,
        s.max_adjuster_level,
        s.adjuster_level_minus_max,
        s.claim_claim_type_count,
        a.team AS adjuster_team_2,
        a.adjuster_name AS adjuster_adjuster_name,
        p.end_date AS policy_end_date_2
    FROM summarized_rows s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
unique_rows AS (
    -- Keep the latest row per adjuster_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claim_type_slug,
        adjuster_team,
        adjuster_level,
        policy_end_date,
        claim_type_slug_2,
        policy_end_date_label,
        max_adjuster_level,
        adjuster_level_minus_max,
        claim_claim_type_count,
        adjuster_team_2,
        adjuster_adjuster_name,
        policy_end_date_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id ORDER BY policy_end_date_2 DESC, id DESC) AS row_num
        FROM combined_details
    ) ranked
    WHERE row_num = 1
)
-- Subtotals with ROLLUP
SELECT
    adjuster_id,
    COALESCE(adjuster_team_2, 'ALL') AS adjuster_team_2,
    PERCENTILE_APPROX(max_adjuster_level, 0.5) AS median_max_adjuster_level
FROM unique_rows
GROUP BY ROLLUP(adjuster_id, adjuster_team_2)
ORDER BY adjuster_id NULLS LAST, adjuster_team_2 NULLS LAST
