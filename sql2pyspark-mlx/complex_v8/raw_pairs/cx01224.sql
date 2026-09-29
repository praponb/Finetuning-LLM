WITH cleaned_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        priority,
        csat,
        tags,
        DATEDIFF(DATE '2025-12-31', opened_at) AS days_since_opened_at,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug
    FROM tickets
    WHERE priority IN ('P2', 'P4')
      AND csat <= 4
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.tags,
        s.days_since_opened_at,
        t.id AS event_id,
        t.event_at,
        t.event_type,
        t.actor AS event_actor
    FROM cleaned_tickets s
    JOIN ticket_events t ON t.ticket_id = s.id AND t.event_type NOT IN ('created', 'reopened')
),
agent_id_stats AS (
    -- Avg days_since_opened_at per agent_id
    SELECT agent_id, AVG(days_since_opened_at) AS avg_days_since_opened_at
    FROM detail_rows_events
    GROUP BY agent_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_days_since_opened_at,
        ROUND(s.days_since_opened_at - g.avg_days_since_opened_at, 2) AS days_since_opened_at_minus_avg
    FROM detail_rows_events s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.days_since_opened_at > g.avg_days_since_opened_at
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        tags,
        days_since_opened_at,
        event_id,
        event_at,
        event_type,
        event_actor,
        avg_days_since_opened_at,
        days_since_opened_at_minus_avg,
        COUNT(*) OVER (PARTITION BY event_type) AS rows_in_event_type,
        ROUND(days_since_opened_at / NULLIF(SUM(days_since_opened_at) OVER (PARTITION BY event_type), 0), 4) AS days_since_opened_at_share_of_event_type,
        LAST_VALUE(event_at) OVER (ORDER BY days_since_opened_at DESC, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at
    FROM above_average
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER w_event_type_by_days_since_ope AS days_since_opened_at_minus_avg_dense_rank,
        ROUND(avg_days_since_opened_at / NULLIF(SUM(avg_days_since_opened_at) OVER (PARTITION BY YEAR(last_event_at)), 0), 4) AS avg_days_since_opened_at_share_of_last_event_at_,
        ROUND(AVG(avg_days_since_opened_at) OVER (PARTITION BY event_actor ORDER BY last_event_at DESC, id, event_id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS avg_days_since_opened_at_avg_2
    FROM sequenced
    WINDOW w_event_type_by_days_since_ope AS (PARTITION BY event_type ORDER BY days_since_opened_at_minus_avg)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN days_since_opened_at >= 490 THEN 'HIGH' WHEN days_since_opened_at >= 280 THEN 'MEDIUM' ELSE 'LOW' END AS days_since_opened_at_tier,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        event_at + INTERVAL 2 HOURS AS event_at_plus_2h
    FROM metrics
)
SELECT
    id,
    tags,
    days_since_opened_at,
    event_id,
    event_at,
    event_type,
    event_actor,
    avg_days_since_opened_at,
    days_since_opened_at_minus_avg,
    rows_in_event_type,
    days_since_opened_at_share_of_event_type,
    last_event_at,
    days_since_opened_at_minus_avg_dense_rank,
    avg_days_since_opened_at_share_of_last_event_at_,
    avg_days_since_opened_at_avg_2,
    days_since_opened_at_tier,
    event_type_or_unknown
FROM derived
WHERE days_since_opened_at_minus_avg_dense_rank >= 3
ORDER BY event_actor DESC, event_type DESC
