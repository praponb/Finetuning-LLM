WITH recent_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        tags
    FROM tickets
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.tags,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type,
        t.event_at
    FROM recent_tickets s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
),
grouped AS (
    -- Group-level totals
    SELECT
        event_type,
        event_actor,
        MAX(event_at) AS last_event_at,
        ROUND(100 * COUNT(CASE WHEN event_actor = 'customer' THEN 1 END) / COUNT(*), 1) AS pct_event_actor_customer,
        MAX(opened_at) AS last_opened_at,
        COUNT(*) AS cnt
    FROM detail_rows_events
    GROUP BY event_type, event_actor
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM grouped
    WHERE pct_event_actor_customer < (SELECT MAX(pct_event_actor_customer) FROM grouped)
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM without_matches
    WHERE cnt > (SELECT MIN(cnt) FROM without_matches)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER w_event_type_last_opened_at_ye AS rows_in_event_type_last_opened_at_year,
        ROUND(cnt - AVG(cnt) OVER (PARTITION BY event_type, YEAR(last_opened_at)), 2) AS cnt_vs_event_type_last_opened_at_year_avg,
        MIN(pct_event_actor_customer) OVER (ORDER BY pct_event_actor_customer DESC, event_type, event_actor ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_pct_event_actor_customer_so_far,
        ROUND(AVG(cnt) OVER (PARTITION BY event_type, YEAR(last_opened_at)), 2) AS event_type_last_opened_at_year_avg_cnt
    FROM screened
    WINDOW w_event_type_last_opened_at_ye AS (PARTITION BY event_type, YEAR(last_opened_at))
)
SELECT
    event_type,
    pct_event_actor_customer,
    last_opened_at,
    rows_in_event_type_last_opened_at_year,
    cnt_vs_event_type_last_opened_at_year_avg,
    min_pct_event_actor_customer_so_far,
    COALESCE(cnt, 0) AS cnt_filled
FROM ranked
ORDER BY cnt_vs_event_type_last_opened_at_year_avg, cnt_filled NULLS LAST
