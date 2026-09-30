WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        QUARTER(event_at) AS event_at_quarter
    FROM ticket_events
    WHERE ticket_id >= 21
),
actor_stats AS (
    -- Max event_at_quarter per actor
    SELECT actor, MAX(event_at_quarter) AS max_event_at_quarter
    FROM valid
    GROUP BY actor
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_event_at_quarter,
        ROUND(s.event_at_quarter - g.max_event_at_quarter, 2) AS event_at_quarter_minus_max
    FROM valid s
    JOIN actor_stats g ON g.actor = s.actor
    WHERE s.event_at_quarter = g.max_event_at_quarter
),
latest AS (
    -- Keep the earliest row per event_type, ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_quarter,
        max_event_at_quarter,
        event_at_quarter_minus_max
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type, ticket_id ORDER BY event_at, id) AS rn
        FROM compared
    ) ranked
    WHERE rn = 1
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_at_quarter,
        max_event_at_quarter,
        event_at_quarter_minus_max,
        SUBSTRING(TRIM(actor), 1, 4) AS actor_prefix
    FROM latest
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at_2
    FROM derived s
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        DAYOFWEEK(event_at) AS event_at_dayofweek,
        ROUND(max_event_at_quarter * 1.5, 2) AS max_event_at_quarter_adj
    FROM summarized_rows
),
ticket_id_stats AS (
    -- Avg max_event_at_quarter_adj per ticket_id
    SELECT ticket_id, AVG(max_event_at_quarter_adj) AS avg_max_event_at_quarter_adj
    FROM tagged
    GROUP BY ticket_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_max_event_at_quarter_adj,
        ROUND(s.max_event_at_quarter_adj - g.avg_max_event_at_quarter_adj, 2) AS max_event_at_quarter_adj_minus_avg
    FROM tagged s
    JOIN ticket_id_stats g ON g.ticket_id = s.ticket_id
    WHERE s.max_event_at_quarter_adj > g.avg_max_event_at_quarter_adj
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    max_event_at_quarter,
    event_at_quarter_minus_max,
    actor_prefix,
    event_count,
    last_event_at,
    event_at_dayofweek,
    max_event_at_quarter_adj,
    avg_max_event_at_quarter_adj,
    ROUND(event_count / NULLIF(event_at_quarter, 0), 3) AS event_count_per_event_at_quarter,
    ROUND(max_event_at_quarter / NULLIF(event_count, 0), 3) AS max_event_at_quarter_per_event_count
FROM above_average
ORDER BY id DESC
LIMIT 20
