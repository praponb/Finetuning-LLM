WITH filtered AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date,
        UPPER(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
    WHERE actor IN ('agent', 'customer')
      AND event_type NOT IN ('replied', 'created')
),
with_parent_ticket AS (
    -- Join tickets
    SELECT
        s.*,
        t.tags AS ticket_tags,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM filtered s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY event_at ASC, id) AS event_at_seq,
        ROUND(CUME_DIST() OVER (PARTITION BY actor, MONTH(event_at_date) ORDER BY event_at), 3) AS event_at_cume_dist,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor, MONTH(event_at_date) ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor, MONTH(event_at_date) ORDER BY event_at, id), 3) AS event_at_percent_rank,
        NTILE(3) OVER w_actor_event_at_date_month_by_3 AS event_at_tercile,
        NTILE(5) OVER (PARTITION BY actor, agent_team ORDER BY event_at_date DESC, id) AS event_at_date_quintile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor, agent_team ORDER BY event_at_date DESC), 3) AS event_at_date_percent_rank,
        DATEDIFF(event_at_date, LAG(event_at_date) OVER (ORDER BY event_at ASC, id)) AS days_since_prev_event_at_date,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY actor, agent_team ORDER BY event_at_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        LAG(event_at, 3) OVER w_all_by_event_at AS prev3_event_at,
        FIRST_VALUE(event_at_date) IGNORE NULLS OVER (PARTITION BY actor, MONTH(event_at_date) ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_date,
        NTH_VALUE(event_at_date, 2) OVER (ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_date
    FROM with_parent_ticket
    WINDOW w_actor_event_at_date_month_by_3 AS (PARTITION BY actor, MONTH(event_at_date) ORDER BY event_at, id), w_all_by_event_at AS (ORDER BY event_at ASC, id)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(event_type_clean), ' ', '_')) AS event_type_clean_slug
    FROM sequenced
)
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    event_at_date,
    event_type_clean,
    ticket_tags,
    agent_tier,
    event_at_seq,
    event_at_cume_dist,
    event_at_percent_rank,
    event_at_tercile,
    event_at_date_quintile,
    days_since_prev_event_at_date,
    second_event_at_2,
    prev3_event_at,
    first_known_event_at_date,
    second_event_at_date,
    COALESCE(agent_tier, 'UNKNOWN') AS agent_tier_or_unknown
FROM annotated
