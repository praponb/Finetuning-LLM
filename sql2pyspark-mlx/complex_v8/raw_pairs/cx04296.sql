WITH valid_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        DAYOFWEEK(closed_at) AS closed_at_dayofweek,
        CONCAT(channel, '#', CAST(id AS STRING)) AS channel_ref,
        ARRAY_JOIN(ARRAY_SORT(tags), ',') AS tags_text
    FROM tickets
    WHERE (agent_id >= 6 OR requester_id > 7)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        channel_ref,
        tags_text,
        RANK() OVER (PARTITION BY agent_id ORDER BY closed_at DESC) AS closed_at_rank,
        MIN(closed_at_dayofweek) OVER (PARTITION BY csat) AS csat_min_closed_at_dayofweek,
        closed_at_dayofweek - LAG(closed_at_dayofweek) OVER w_csat_by_opened_at AS closed_at_dayofweek_change
    FROM valid_tickets
    WINDOW w_csat_by_opened_at AS (PARTITION BY csat ORDER BY opened_at ASC, id)
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.csat,
        s.tags,
        s.channel_ref,
        s.tags_text,
        s.closed_at_rank,
        t.id AS event_id,
        t.event_at,
        t.event_type,
        t.actor AS event_actor
    FROM windowed s
    JOIN ticket_events t ON t.ticket_id = s.id
),
stats AS (
    -- Summarise per group
    SELECT
        event_id,
        MIN(event_at) AS first_event_at,
        SUM(closed_at_rank) AS total_closed_at_rank,
        MAX(event_at) AS last_event_at
    FROM exploded_detail_events
    GROUP BY event_id
    HAVING SUM(csat) > 2
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(last_event_at, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', first_event_at) ORDER BY total_closed_at_rank, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_event_at,
        NTILE(5) OVER (PARTITION BY DATE_TRUNC('MONTH', first_event_at) ORDER BY total_closed_at_rank, event_id) AS total_closed_at_rank_quintile,
        ROUND(total_closed_at_rank / NULLIF(SUM(total_closed_at_rank) OVER (PARTITION BY YEAR(last_event_at), event_id), 0), 4) AS total_closed_at_rank_share_of_last_event_at_year
    FROM stats
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        MONTH(last_event_at) AS last_event_at_month,
        DATE_TRUNC('MONTH', first_event_at) AS first_event_at_month
    FROM metrics
)
SELECT
    event_id,
    first_event_at,
    last_event_at,
    total_closed_at_rank_quintile,
    total_closed_at_rank_share_of_last_event_at_year,
    last_event_at_month,
    first_event_at_month,
    DATE_TRUNC('HOUR', second_last_event_at) AS second_last_event_at_hour,
    ABS(last_event_at_month - 6) AS last_event_at_month_distance
FROM annotated
WHERE last_event_at_month <= 10
ORDER BY last_event_at_month_distance NULLS LAST
