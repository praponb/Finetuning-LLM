WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('|', actor, event_type) AS actor_event_type_label,
        LENGTH(TRIM(actor)) AS actor_len,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label
    FROM ticket_events
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_event_type_label,
        event_at_label,
        DATE_TRUNC('WEEK', event_at) AS event_at_week,
        FLOOR(actor_len / 2) * 2 AS actor_len_bucket
    FROM cleaned
    WHERE event_type IN ('escalated', 'replied')
),
with_parent_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.*,
        t.agent_id AS ticket_agent_id,
        t.tags AS ticket_tags,
        t.priority AS ticket_priority,
        su.tier AS agent_tier
    FROM derived s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY actor ORDER BY event_at_week ASC, id) AS event_at_week_quintile,
        MAX(actor_len_bucket) OVER (PARTITION BY ticket_priority, event_type ORDER BY event_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS actor_len_bucket_max_6_prior,
        SIZE(COLLECT_SET(agent_tier) OVER (PARTITION BY ticket_id)) AS distinct_agent_tier_in_ticket_id,
        NTH_VALUE(actor_len_bucket, 2) OVER (PARTITION BY ticket_priority, event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_actor_len_bucket
    FROM with_parent_ticket
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        event_type,
        actor,
        event_at_week,
        ticket_agent_id,
        ticket_tags,
        agent_tier,
        actor_len_bucket_max_6_prior,
        distinct_agent_tier_in_ticket_id,
        second_actor_len_bucket,
        DENSE_RANK() OVER (ORDER BY second_actor_len_bucket DESC, id) AS second_actor_len_bucket_dense_rank,
        LEAD(event_at) OVER (PARTITION BY TO_DATE(event_at_week), second_actor_len_bucket ORDER BY event_at, id) AS next_event_at,
        ROUND(AVG(actor_len_bucket_max_6_prior) OVER (PARTITION BY TO_DATE(event_at_week), second_actor_len_bucket ORDER BY event_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS actor_len_bucket_max_6_prior_avg_2_prior
    FROM with_trends
)
SELECT
    id,
    event_at,
    event_type,
    actor,
    event_at_week,
    ticket_tags,
    agent_tier,
    actor_len_bucket_max_6_prior,
    distinct_agent_tier_in_ticket_id,
    second_actor_len_bucket,
    second_actor_len_bucket_dense_rank,
    next_event_at,
    actor_len_bucket_max_6_prior_avg_2_prior,
    COALESCE(second_actor_len_bucket, 0) AS second_actor_len_bucket_filled,
    YEAR(event_at_week) AS event_at_week_year
FROM sequenced
WHERE HOUR(event_at) BETWEEN 10 AND 12
ORDER BY actor NULLS LAST, event_at_week_year DESC
