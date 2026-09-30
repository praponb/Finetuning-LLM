WITH base_agents AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        DATE_FORMAT(hired_on, 'yyyy-MM') AS hired_on_label,
        COALESCE(tier, 'UNKNOWN') AS tier_or_unknown
    FROM support_agents
),
summary AS (
    -- Summarise per group
    SELECT
        tier_or_unknown,
        DATE_FORMAT(hired_on, 'yyyy-MM') AS hired_on_ym,
        team,
        COUNT(*) AS row_count,
        COUNT(DISTINCT tier_or_unknown) AS distinct_tier_or_unknowns,
        MIN(hired_on) AS first_hired_on,
        COUNT(DISTINCT tier) AS distinct_tiers
    FROM base_agents
    WHERE hired_on >= DATE '2024-05-02'
    GROUP BY tier_or_unknown, DATE_FORMAT(hired_on, 'yyyy-MM'), team
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(tier_or_unknown), ' ', '_')) AS tier_or_unknown_slug,
        DATEDIFF(DATE '2025-06-30', first_hired_on) AS days_since_first_hired_on,
        LOWER(REGEXP_REPLACE(TRIM(tier_or_unknown), ' ', '_')) AS tier_or_unknown_slug_2
    FROM summary
)
-- Final projection
SELECT
    tier_or_unknown,
    hired_on_ym,
    team,
    row_count,
    distinct_tier_or_unknowns,
    first_hired_on,
    distinct_tiers,
    days_since_first_hired_on,
    tier_or_unknown_slug_2,
    LOWER(REGEXP_REPLACE(TRIM(team), ' ', '_')) AS team_slug
FROM annotated
WHERE days_since_first_hired_on > 210
