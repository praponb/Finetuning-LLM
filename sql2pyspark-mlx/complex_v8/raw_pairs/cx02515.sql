WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        channel,
        csat,
        tags,
        SUBSTRING(TRIM(channel), 1, 4) AS channel_prefix,
        TO_DATE(closed_at) AS closed_at_date
    FROM tickets
    WHERE channel NOT IN ('email', 'web')
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(csat) OVER (PARTITION BY agent_id) AS agent_id_max_csat,
        ROUND(CUME_DIST() OVER w_all_by_csat, 3) AS csat_cume_dist,
        DATEDIFF(closed_at_date, LAG(closed_at_date) OVER (ORDER BY csat, id)) AS days_since_prev_closed_at_date,
        DATEDIFF(closed_at_date, LAG(closed_at_date) OVER (PARTITION BY agent_id ORDER BY csat ASC, id)) AS days_since_prev_closed_at_date_2,
        COUNT(*) OVER w_agent_id AS rows_in_agent_id,
        NTH_VALUE(closed_at_date, 2) OVER (PARTITION BY agent_id ORDER BY csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at_date
    FROM prepared
    WINDOW w_all_by_csat AS (ORDER BY csat, id), w_agent_id AS (PARTITION BY agent_id)
),
with_activity AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.actor IN ('agent', 'system')
              AND t.event_type = 'resolved'
        )
)
SELECT *
FROM (
    SELECT closed_at_date, channel, csat
    FROM with_activity
) src
PIVOT (
    SUM(csat)
    FOR channel IN ('phone', 'email')
)
ORDER BY closed_at_date
