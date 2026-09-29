WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        COALESCE(tier, 'UNKNOWN') AS tier_or_unknown
    FROM support_agents
    WHERE tier <> 'T1'
),
qualified AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.id
              AND TO_DATE(t.closed_at) < DATE '2025-02-26'
              AND t.channel IN ('web', 'phone')
        )
),
deduped AS (
    -- Keep the latest row per team
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        tier_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY team ORDER BY hired_on DESC, id DESC) AS rn
        FROM qualified
    ) ranked
    WHERE rn = 1
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(hired_on, 'yyyy-MM') AS hired_on_ym,
        ROUND(100 * COUNT(CASE WHEN UPPER(TRIM(agent_name)) LIKE 'J%' THEN 1 END) / COUNT(*), 1) AS pct_upper_trim_agent_name_like_j,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE UPPER(TRIM(agent_name)) LIKE 'L%') AS n_upper_trim_agent_name_like_l,
        MIN(hired_on) AS first_hired_on,
        COUNT(*) AS cnt
    FROM deduped
    GROUP BY DATE_FORMAT(hired_on, 'yyyy-MM')
)
SELECT hired_on_ym, metric_name, reading
FROM (
    SELECT hired_on_ym, n_upper_trim_agent_name_like_l, distinct_ids, cnt
    FROM grouped
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric_name IN (n_upper_trim_agent_name_like_l AS n_upper_trim_agent_name_like_l_v, distinct_ids AS distinct_ids_v, cnt AS cnt_v)
) unp
ORDER BY hired_on_ym, metric_name
