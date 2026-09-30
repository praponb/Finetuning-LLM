WITH recent_agents AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on
    FROM support_agents
    WHERE team = 'Onboarding'
),
latest AS (
    -- Keep the earliest row per team, tier
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY team, tier ORDER BY hired_on, id) AS rn
        FROM recent_agents
    ) ranked
    WHERE rn = 1
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        tier,
        hired_on,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(hired_on) ORDER BY hired_on, id), 3) AS hired_on_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY YEAR(hired_on) ORDER BY hired_on, id) AS seq_no,
        FIRST_VALUE(hired_on) IGNORE NULLS OVER (PARTITION BY YEAR(hired_on) ORDER BY hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_hired_on
    FROM latest
    WINDOW w_hired_on_year_by_hired_on AS (PARTITION BY YEAR(hired_on) ORDER BY hired_on, id)
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(tier), 1, 4) AS tier_prefix,
        DATE_FORMAT(first_known_hired_on, 'yyyy-MM-dd') AS first_known_hired_on_label,
        CONCAT(tier, '#', CAST(seq_no AS STRING)) AS tier_ref
    FROM windowed
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', hired_on)) AS rows_in_hired_on_month,
        MAX(seq_no) OVER (PARTITION BY tier, seq_no ORDER BY hired_on, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS seq_no_max_2_prior,
        SUM(hired_on_cume_dist) OVER (PARTITION BY DATE_TRUNC('MONTH', hired_on)) AS hired_on_month_sum_hired_on_cume_dist
    FROM scored
),
tier_stats AS (
    -- Avg seq_no per tier
    SELECT tier, AVG(seq_no) AS avg_seq_no
    FROM with_windows
    GROUP BY tier
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_seq_no, ROUND(s.seq_no - g.avg_seq_no, 2) AS seq_no_minus_avg
    FROM with_windows s
    JOIN tier_stats g ON g.tier = s.tier
    WHERE s.seq_no > g.avg_seq_no
),
detail_rows_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.tier,
        s.hired_on,
        s.hired_on_cume_dist,
        s.seq_no,
        s.first_known_hired_on,
        s.tier_prefix,
        s.first_known_hired_on_label,
        s.tier_ref,
        s.avg_seq_no,
        t.id AS ticket_id,
        t.csat AS ticket_csat,
        t.priority AS ticket_priority
    FROM compared s
    JOIN tickets t ON t.agent_id = s.id AND t.priority NOT IN ('P4', 'P2')
)
-- Final projection
SELECT
    id,
    tier,
    hired_on,
    hired_on_cume_dist,
    seq_no,
    first_known_hired_on,
    tier_prefix,
    first_known_hired_on_label,
    tier_ref,
    avg_seq_no,
    ticket_id,
    ticket_csat,
    ticket_priority
FROM detail_rows_tickets
WHERE ticket_csat BETWEEN 2 AND 5
ORDER BY tier_ref, ticket_id NULLS LAST, id
LIMIT 20
