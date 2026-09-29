WITH scoped_agents AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        SUBSTRING(TRIM(team), 1, 4) AS team_prefix,
        LOWER(REGEXP_REPLACE(TRIM(tier), ' ', '_')) AS tier_slug
    FROM support_agents
    WHERE team = 'Billing'
      AND UPPER(TRIM(agent_name)) LIKE 'I%'
),
detail_rows_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.agent_name,
        s.team,
        s.tier,
        s.hired_on,
        t.id AS ticket_id,
        t.priority AS ticket_priority,
        t.closed_at AS ticket_closed_at
    FROM scoped_agents s
    JOIN tickets t ON t.agent_id = s.id
),
with_details_ticket AS (
    -- Attach ticket attributes
    SELECT s.*, t.requester_id AS ticket_requester_id
    FROM detail_rows_tickets s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND t.priority <> 'P4'
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(hired_on, 'MMM yyyy') AS hired_on_label,
        ticket_closed_at + INTERVAL 2 HOURS AS ticket_closed_at_plus_2h,
        CONCAT(tier, '#', CAST(id AS STRING)) AS tier_ref
    FROM with_details_ticket
    WHERE tier = 'T3'
),
unique_rows AS (
    -- Keep the latest row per tier
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        ticket_id,
        ticket_priority,
        ticket_closed_at,
        ticket_requester_id,
        hired_on_label,
        ticket_closed_at_plus_2h,
        tier_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tier ORDER BY hired_on DESC, id DESC, ticket_id DESC) AS rn
        FROM scored
    ) ranked
    WHERE rn = 1
),
joined_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.agent_name,
        s.team,
        s.ticket_id,
        s.ticket_closed_at,
        s.ticket_requester_id,
        s.hired_on_label,
        s.ticket_closed_at_plus_2h,
        s.tier_ref,
        t.agent_id AS ticket_agent_id
    FROM unique_rows s
    JOIN tickets t ON t.id = s.ticket_id
),
grouped AS (
    -- Group-level totals
    SELECT
        ticket_id,
        ticket_agent_id,
        SUM(CASE WHEN UPPER(TRIM(agent_name)) LIKE 'K%' THEN ticket_requester_id ELSE 0 END) AS ticket_requester_id_upper_trim_agent_name_like_k,
        COUNT(*) AS n_rows,
        ARRAY_SORT(COLLECT_SET(agent_name)) AS agent_name_list,
        ROUND(AVG(ticket_requester_id), 2) AS avg_ticket_requester_id
    FROM joined_ticket
    GROUP BY ticket_id, ticket_agent_id
    HAVING AVG(ticket_requester_id) > 7
)
SELECT
    ticket_id,
    ticket_agent_id,
    n_rows,
    ROUND(STDDEV(avg_ticket_requester_id), 2) AS stddev_avg_ticket_requester_id
FROM grouped
GROUP BY GROUPING SETS ((ticket_id, ticket_agent_id, n_rows), (ticket_id), ())
ORDER BY ticket_id NULLS LAST, ticket_agent_id NULLS LAST, n_rows NULLS LAST
