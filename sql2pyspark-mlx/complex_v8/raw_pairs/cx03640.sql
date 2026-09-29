WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        actor_or_unknown,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label,
        DATE_TRUNC('MONTH', event_at) AS event_at_month
    FROM valid
    WHERE ticket_id <= 37
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        ticket_id,
        MIN(event_at_month) AS first_event_at_month,
        MAX(event_at_month) AS last_event_at_month
    FROM labeled
    WHERE event_at_label IS NOT NULL
    GROUP BY ticket_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(last_event_at_month, 'yyyy-MM-dd') AS last_event_at_month_label,
        ROUND((UNIX_TIMESTAMP(first_event_at_month) - UNIX_TIMESTAMP(last_event_at_month)) / 3600, 1) AS hours_last_event_at_month_to_first_event_at_mont
    FROM rollup_base
    WHERE HOUR(last_event_at_month) BETWEEN 6 AND 9
),
enriched AS (
    -- Attach ticket attributes
    SELECT
        s.ticket_id,
        s.first_event_at_month,
        s.last_event_at_month_label,
        s.hours_last_event_at_month_to_first_event_at_mont,
        t.channel AS ticket_channel,
        t.csat AS ticket_csat,
        t.requester_id AS ticket_requester_id,
        su.agent_name,
        su.team AS agent_team
    FROM annotated s
    JOIN tickets t ON t.id = s.ticket_id AND t.opened_at < TIMESTAMP '2025-02-24 15:00:00'
    LEFT JOIN support_agents su ON su.id = t.agent_id
)
SELECT *
FROM (
    SELECT ticket_csat, ticket_channel, hours_last_event_at_month_to_first_event_at_mont
    FROM enriched
) src
PIVOT (
    MAX(hours_last_event_at_month_to_first_event_at_mont)
    FOR ticket_channel IN ('email', 'web')
)
