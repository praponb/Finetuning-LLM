WITH filtered AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        DATEDIFF(DATE '2025-06-30', closed_at) AS days_since_closed_at
    FROM tickets
    WHERE (channel <> 'phone' OR agent_id IS NOT NULL)
),
with_details AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.days_since_closed_at,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM filtered s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
totals AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', agent_hired_on) AS agent_hired_on_month,
        YEAR(closed_at) AS closed_at_year,
        channel,
        COUNT(*) FILTER (WHERE priority = 'P4') AS n_priority_p4,
        COUNT(*) FILTER (WHERE priority IN ('P3', 'P1')) AS n_priority_in_p3_p1
    FROM with_details
    GROUP BY 1, 2, 3
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY n_priority_p4, YEAR(agent_hired_on_month) ORDER BY n_priority_in_p3_p1 DESC, agent_hired_on_month, closed_at_year, channel) AS n_priority_in_p3_p1_rank,
        ROW_NUMBER() OVER (ORDER BY n_priority_in_p3_p1 ASC, agent_hired_on_month, closed_at_year, channel) AS rn,
        COUNT(*) OVER (PARTITION BY closed_at_year, n_priority_p4) AS rows_in_closed_at_year_n_priority_p4,
        LEAD(n_priority_in_p3_p1) OVER (PARTITION BY closed_at_year, n_priority_p4 ORDER BY n_priority_p4, agent_hired_on_month, closed_at_year, channel) AS next_n_priority_in_p3_p1
    FROM totals
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug,
        DATE_FORMAT(agent_hired_on_month, 'yyyy-MM-dd HH:00') AS agent_hired_on_month_label,
        DATE_TRUNC('HOUR', agent_hired_on_month) AS agent_hired_on_month_hour
    FROM sequenced
),
enriched_2 AS (
    -- Label and score rows
    SELECT
        *,
        ABS(closed_at_year - 2025) AS closed_at_year_distance,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug_2,
        ABS(n_priority_in_p3_p1 - 3) AS n_priority_in_p3_p1_distance
    FROM enriched
    WHERE next_n_priority_in_p3_p1 >= 2
)
SELECT *
FROM (
    SELECT n_priority_in_p3_p1_rank, n_priority_p4, channel, next_n_priority_in_p3_p1
    FROM enriched_2
) src
PIVOT (
    MAX(next_n_priority_in_p3_p1)
    FOR channel IN ('email', 'chat', 'phone', 'web')
)
ORDER BY n_priority_in_p3_p1_rank, n_priority_p4
