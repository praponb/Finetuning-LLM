WITH filtered AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        tier,
        LOWER(REGEXP_REPLACE(TRIM(tier), ' ', '_')) AS tier_slug,
        LENGTH(TRIM(team)) AS team_len,
        SUBSTRING(TRIM(team), 1, 4) AS team_prefix
    FROM support_agents
    WHERE agent_name IS NOT NULL
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        agent_name,
        team_len,
        team_prefix,
        COALESCE(tier, 'UNKNOWN') AS tier_or_unknown
    FROM filtered
),
filtered_2 AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM annotated s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.id
              AND t.channel = 'chat'
        )
),
totals AS (
    -- Summarise per group
    SELECT
        team_len,
        COUNT(DISTINCT tier_or_unknown) AS distinct_tier_or_unknowns,
        ARRAY_SORT(COLLECT_SET(agent_name)) AS agent_name_list,
        COUNT(*) AS cnt
    FROM filtered_2
    WHERE UPPER(TRIM(agent_name)) LIKE 'J%'
    GROUP BY 1
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(distinct_tier_or_unknowns) OVER (PARTITION BY cnt, team_len), 2) AS cnt_team_len_avg_distinct_tier_or_unknowns,
        MIN(cnt) OVER (PARTITION BY cnt, team_len) AS cnt_team_len_min_cnt,
        MAX(distinct_tier_or_unknowns) OVER (PARTITION BY team_len, cnt ORDER BY distinct_tier_or_unknowns ASC, team_len ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS distinct_tier_or_unknowns_max_3,
        ROUND(distinct_tier_or_unknowns / NULLIF(SUM(distinct_tier_or_unknowns) OVER w_cnt_team_len, 0), 4) AS distinct_tier_or_unknowns_share_of_cnt_team_len,
        COUNT(*) OVER (PARTITION BY cnt, team_len) AS rows_in_cnt_team_len
    FROM totals
    WINDOW w_cnt_team_len AS (PARTITION BY cnt, team_len)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(cnt_team_len_min_cnt / 25) * 25 AS cnt_team_len_min_cnt_bucket,
        CASE WHEN cnt >= 9 THEN 'A' WHEN cnt >= 4 THEN 'B' ELSE 'C' END AS cnt_tier,
        SIZE(agent_name_list) AS agent_name_list_count
    FROM ranked
    WHERE rows_in_cnt_team_len <= 11
),
scored AS (
    -- Derive helper columns
    SELECT
        team_len,
        distinct_tier_or_unknowns,
        agent_name_list,
        cnt,
        cnt_team_len_avg_distinct_tier_or_unknowns,
        distinct_tier_or_unknowns_max_3,
        cnt_team_len_min_cnt_bucket,
        cnt_tier,
        agent_name_list_count,
        LOWER(TRIM(cnt_tier)) AS cnt_tier_clean
    FROM labeled
)
-- Native SQL UNPIVOT (wide to long)
SELECT team_len, field, reading
FROM (
    SELECT team_len, distinct_tier_or_unknowns_max_3, agent_name_list_count, cnt_team_len_min_cnt_bucket, distinct_tier_or_unknowns
    FROM scored
) src
UNPIVOT (
    reading FOR field IN (distinct_tier_or_unknowns_max_3 AS m_distinct_tier_or_unknowns_max_3, agent_name_list_count AS m_agent_name_list_count, cnt_team_len_min_cnt_bucket AS cnt_team_len_min_cnt_bucket_v, distinct_tier_or_unknowns AS m_distinct_tier_or_unknowns)
) unp
