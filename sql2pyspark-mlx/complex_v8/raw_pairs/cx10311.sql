WITH prepared_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(actor)) AS actor_len,
        LENGTH(TRIM(actor)) AS actor_len_2,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE (ticket_id IS NOT NULL OR event_at >= TIMESTAMP '2025-01-25 09:00:00')
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(actor_len_2) OVER (PARTITION BY MONTH(event_at), actor_len_2) AS event_at_month_actor_len_2_max_actor_len_2,
        ROW_NUMBER() OVER w_all_by_actor_len_2 AS rn,
        COUNT(*) OVER w_event_at_month_actor_len_2 AS rows_in_event_at_month_actor_len_2
    FROM prepared_events
    WINDOW w_all_by_actor_len_2 AS (ORDER BY actor_len_2 ASC, id), w_event_at_month_actor_len_2 AS (PARTITION BY MONTH(event_at), actor_len_2)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM windowed
    WHERE rn <= 5
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(rn) OVER (PARTITION BY actor_len, actor ORDER BY rn, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS rn_sum_2_prior,
        LAST_VALUE(actor_len) OVER (PARTITION BY actor_len, actor ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_actor_len,
        ROUND(PERCENT_RANK() OVER w_actor_len_actor_by_rn_ties, 3) AS rn_percent_rank
    FROM first_rows
    WINDOW w_actor_len_actor_by_rn_ties AS (PARTITION BY actor_len, actor ORDER BY rn)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_type,
        actor_len,
        event_type_actor_label,
        event_at_month_actor_len_2_max_actor_len_2,
        rn,
        rows_in_event_at_month_actor_len_2,
        rn_sum_2_prior,
        last_actor_len,
        rn_percent_rank,
        NTILE(4) OVER (ORDER BY event_at_month_actor_len_2_max_actor_len_2, id) AS event_at_month_actor_len_2_max_actor_len_2_quart,
        RANK() OVER w_all_by_rows_in_event_at_mont AS rows_in_event_at_month_actor_len_2_rank,
        ROUND(rn - AVG(rn) OVER (PARTITION BY YEAR(event_at)), 2) AS rn_vs_event_at_year_avg
    FROM ranked
    WINDOW w_all_by_rows_in_event_at_mont AS (ORDER BY rows_in_event_at_month_actor_len_2)
),
with_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor_len,
        s.event_type_actor_label,
        s.event_at_month_actor_len_2_max_actor_len_2,
        s.rn,
        s.rows_in_event_at_month_actor_len_2,
        s.last_actor_len,
        s.rn_percent_rank,
        s.event_at_month_actor_len_2_max_actor_len_2_quart,
        s.rows_in_event_at_month_actor_len_2_rank,
        s.rn_vs_event_at_year_avg,
        t.channel AS ticket_channel,
        t.tags AS ticket_tags,
        t.opened_at AS ticket_opened_at,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM sequenced s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.event_at_month_actor_len_2_max_actor_len_2,
        s.rn_percent_rank,
        s.rn_vs_event_at_year_avg,
        s.ticket_channel,
        s.ticket_tags,
        s.ticket_opened_at,
        s.agent_team,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.event_at
    FROM with_details_ticket s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_type IS NOT NULL
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    event_at_month_actor_len_2_max_actor_len_2,
    rn_percent_rank,
    rn_vs_event_at_year_avg,
    ticket_channel,
    ticket_tags,
    ticket_opened_at,
    agent_team,
    event_event_type,
    DATEDIFF(DATE '2025-06-30', ticket_opened_at) AS days_since_ticket_opened_at,
    CASE WHEN event_type IN ('assigned', 'reopened') THEN 'group_a' WHEN event_type = 'escalated' THEN 'group_b' ELSE 'other' END AS event_type_group
FROM line_level
ORDER BY id, event_at_month_actor_len_2_max_actor_len_2
