WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        CASE WHEN tier IN ('T3', 'T2') THEN 'group_a' WHEN tier = 'T1' THEN 'group_b' ELSE 'other' END AS tier_group,
        LOWER(REGEXP_REPLACE(TRIM(team), ' ', '_')) AS team_slug,
        DATEDIFF(DATE '2025-06-30', hired_on) AS days_since_hired_on
    FROM support_agents
    WHERE UPPER(TRIM(agent_name)) LIKE 'I%'
),
tier_stats AS (
    -- Max days_since_hired_on per tier
    SELECT tier, MAX(days_since_hired_on) AS max_days_since_hired_on
    FROM base
    GROUP BY tier
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_days_since_hired_on,
        ROUND(s.days_since_hired_on - g.max_days_since_hired_on, 2) AS days_since_hired_on_minus_max
    FROM base s
    JOIN tier_stats g ON g.tier = s.tier
    WHERE s.days_since_hired_on = g.max_days_since_hired_on
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.id) AS ticket_count,
        (SELECT MAX(t.csat) FROM tickets t WHERE t.agent_id = s.id) AS max_ticket_csat
    FROM vs_group s
),
tier_stats_2 AS (
    -- Median ticket_count per tier
    SELECT tier, PERCENTILE_APPROX(ticket_count, 0.5) AS median_ticket_count
    FROM with_counts
    GROUP BY tier
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_ticket_count,
        ROUND(s.ticket_count - g.median_ticket_count, 2) AS ticket_count_minus_median
    FROM with_counts s
    JOIN tier_stats_2 g ON g.tier = s.tier
    WHERE s.ticket_count >= g.median_ticket_count
),
team_stats AS (
    -- Avg max_ticket_csat per team
    SELECT team, AVG(max_ticket_csat) AS avg_max_ticket_csat
    FROM compared
    GROUP BY team
),
compared_2 AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_max_ticket_csat,
        ROUND(s.max_ticket_csat - g.avg_max_ticket_csat, 2) AS max_ticket_csat_minus_avg
    FROM compared s
    JOIN team_stats g ON g.team = s.team
    WHERE s.max_ticket_csat > g.avg_max_ticket_csat
),
unique_rows AS (
    -- Keep the latest row per tier_group
    SELECT
        id,
        agent_name,
        team,
        tier,
        tier_group,
        team_slug,
        days_since_hired_on,
        max_days_since_hired_on,
        days_since_hired_on_minus_max,
        ticket_count,
        max_ticket_csat,
        median_ticket_count,
        ticket_count_minus_median,
        avg_max_ticket_csat,
        max_ticket_csat_minus_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tier_group ORDER BY median_ticket_count DESC, id DESC) AS recency_rank
        FROM compared_2
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    agent_name,
    team,
    tier,
    tier_group,
    team_slug,
    days_since_hired_on,
    max_days_since_hired_on,
    ticket_count,
    max_ticket_csat,
    ticket_count_minus_median,
    avg_max_ticket_csat,
    COALESCE(tier_group, 'UNKNOWN') AS tier_group_or_unknown,
    CASE WHEN median_ticket_count >= 5.2 THEN 'A' WHEN median_ticket_count >= 3.6 THEN 'B' ELSE 'C' END AS median_ticket_count_tier
FROM unique_rows
ORDER BY team_slug DESC, tier_group_or_unknown DESC
