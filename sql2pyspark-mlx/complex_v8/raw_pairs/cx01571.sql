WITH valid_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 4) AS actor_prefix
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND event_type = 'created'
),
with_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_prefix,
        t.tags AS ticket_tags,
        t.requester_id AS ticket_requester_id,
        t.priority AS ticket_priority,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM valid_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        event_type,
        actor,
        actor_prefix,
        ticket_tags,
        ticket_requester_id,
        ticket_priority,
        agent_team,
        ROUND(ticket_requester_id / NULLIF(SUM(ticket_requester_id) OVER (PARTITION BY event_type), 0), 4) AS ticket_requester_id_share_of_event_type,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_all_by_event_at)) / 60, 1) AS minutes_since_prev_event_at,
        ROUND(ticket_requester_id - AVG(ticket_requester_id) OVER (PARTITION BY agent_tier, ticket_id), 2) AS ticket_requester_id_vs_agent_tier_ticket_id_avg,
        NTILE(5) OVER (PARTITION BY ticket_priority, actor ORDER BY event_at, id) AS event_at_quintile,
        SIZE(COLLECT_SET(actor) OVER w_event_type) AS distinct_actor_in_event_type,
        MAX(ticket_requester_id) OVER (PARTITION BY agent_tier, ticket_id) AS agent_tier_ticket_id_max_ticket_requester_id,
        SUM(ticket_requester_id) OVER w_agent_tier_ticket_id AS agent_tier_ticket_id_sum_ticket_requester_id
    FROM with_details_ticket
    WINDOW w_all_by_event_at AS (ORDER BY event_at, id), w_event_type AS (PARTITION BY event_type), w_agent_tier_ticket_id AS (PARTITION BY agent_tier, ticket_id)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        actor,
        ROUND(STDDEV(ticket_requester_id), 2) AS stddev_ticket_requester_id,
        ROUND(STDDEV(agent_tier_ticket_id_sum_ticket_requester_id), 2) AS stddev_agent_tier_ticket_id_sum_ticket_requester,
        COUNT(*) AS cnt,
        MIN(ticket_requester_id_share_of_event_type) AS min_ticket_requester_id_share_of_event_type,
        ROUND(AVG(distinct_actor_in_event_type), 2) AS avg_distinct_actor_in_event_type
    FROM metrics
    GROUP BY actor
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        actor,
        stddev_ticket_requester_id,
        stddev_agent_tier_ticket_id_sum_ticket_requester,
        cnt,
        cnt - LAG(cnt) OVER (PARTITION BY actor, cnt ORDER BY stddev_ticket_requester_id, actor) AS cnt_change,
        ROUND(stddev_ticket_requester_id / NULLIF(SUM(stddev_ticket_requester_id) OVER (PARTITION BY cnt, actor), 0), 4) AS stddev_ticket_requester_id_share_of_cnt_actor,
        SUM(cnt) OVER (PARTITION BY cnt, actor) AS cnt_actor_sum_cnt
    FROM rollup_base
)
SELECT
    actor,
    stddev_ticket_requester_id,
    stddev_agent_tier_ticket_id_sum_ticket_requester,
    cnt_change,
    stddev_ticket_requester_id_share_of_cnt_actor,
    cnt_actor_sum_cnt,
    SUBSTRING(TRIM(actor), 1, 4) AS actor_prefix
FROM sequenced
ORDER BY cnt_actor_sum_cnt DESC NULLS FIRST, stddev_ticket_requester_id_share_of_cnt_actor NULLS LAST
