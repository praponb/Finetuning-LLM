WITH valid_agents AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        tier,
        hired_on
    FROM support_agents
),
unique_rows AS (
    -- Keep the latest row per tier
    SELECT
        id,
        agent_name,
        tier,
        hired_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tier ORDER BY hired_on DESC, id DESC) AS row_num
        FROM valid_agents
    ) ranked
    WHERE row_num = 1
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(hired_on, 'MMM yyyy') AS hired_on_label,
        TRUNC(hired_on, 'MM') AS hired_on_month_start
    FROM unique_rows
),
annotated AS (
    -- Derive helper columns
    SELECT *, SUBSTRING(TRIM(tier), 1, 2) AS tier_prefix
    FROM tagged
    WHERE hired_on_month_start < DATE '2025-08-01'
),
filtered AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM annotated s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.id
              AND t.requester_id >= 9
        )
)
-- Final projection
SELECT
    id,
    hired_on,
    hired_on_label,
    hired_on_month_start,
    LOWER(REGEXP_REPLACE(TRIM(tier), ' ', '_')) AS tier_slug
FROM filtered
ORDER BY tier_slug
