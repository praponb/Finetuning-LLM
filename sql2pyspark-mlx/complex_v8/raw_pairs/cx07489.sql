WITH cleaned AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        SUBSTRING(TRIM(team), 1, 3) AS team_prefix,
        UPPER(TRIM(adjuster_name)) AS adjuster_name_clean,
        LOWER(TRIM(adjuster_name)) AS adjuster_name_clean_2
    FROM adjusters
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.adjuster_id = s.id) AS first_claim_reported_at,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.adjuster_id = s.id) AS last_claim_reported_at,
        (SELECT MIN(c.claim_date) FROM claims c WHERE c.adjuster_id = s.id) AS first_claim_date
    FROM cleaned s
),
summary AS (
    -- Summarise per group
    SELECT
        level,
        DATE_TRUNC('MONTH', last_claim_reported_at) AS last_claim_reported_at_month,
        team,
        ARRAY_SORT(COLLECT_SET(adjuster_name)) AS adjuster_name_list,
        MAX(last_claim_reported_at) AS last_last_claim_reported_at,
        COUNT(*) AS cnt,
        ROUND(AVG(level), 2) AS avg_level,
        SUM(level) FILTER (WHERE team IS NOT NULL) AS level_team_is_not_null
    FROM profiled
    WHERE level < 3
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 2
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(avg_level - AVG(avg_level) OVER (PARTITION BY cnt), 2) AS avg_level_vs_cnt_avg,
        ROUND(cnt - AVG(cnt) OVER w_cnt, 2) AS cnt_vs_cnt_avg
    FROM summary
    WINDOW w_cnt AS (PARTITION BY cnt)
),
derived AS (
    -- Add calculated columns
    SELECT *, INITCAP(TRIM(team)) AS team_clean
    FROM with_windows
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN team IN ('Motor', 'Health') THEN 'group_a' WHEN team = 'Travel' THEN 'group_b' ELSE 'other' END AS team_group,
        ROUND(avg_level_vs_cnt_avg / NULLIF(avg_level, 0), 3) AS avg_level_vs_cnt_avg_per_avg_level
    FROM derived
    WHERE level_team_is_not_null <= 14
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM annotated
    WHERE cnt_vs_cnt_avg > (SELECT AVG(cnt_vs_cnt_avg) FROM annotated)
)
-- Native SQL UNPIVOT (wide to long)
SELECT level, last_claim_reported_at_month, metric, value
FROM (
    SELECT level, last_claim_reported_at_month, avg_level_vs_cnt_avg_per_avg_level, avg_level, avg_level_vs_cnt_avg, cnt_vs_cnt_avg
    FROM with_activity
) src
UNPIVOT (
    value FOR metric IN (avg_level_vs_cnt_avg_per_avg_level, avg_level, avg_level_vs_cnt_avg, cnt_vs_cnt_avg)
) unp
ORDER BY level, last_claim_reported_at_month, metric
