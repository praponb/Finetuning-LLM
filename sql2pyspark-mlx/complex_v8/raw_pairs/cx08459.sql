WITH scoped AS (
    -- Filter tickets
    SELECT *
    FROM tickets
    WHERE requester_id <= 21
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        ROUND(csat - AVG(csat) OVER w_channel_closed_at_to_date, 2) AS csat_vs_channel_closed_at_to_date_avg,
        ROUND(PERCENT_RANK() OVER (ORDER BY closed_at DESC), 3) AS closed_at_percent_rank,
        LAG(requester_id, 2) OVER (ORDER BY closed_at DESC, id) AS prev2_requester_id
    FROM scoped
    WINDOW w_channel_closed_at_to_date AS (PARTITION BY channel, TO_DATE(closed_at))
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT agent_id, channel, prev2_requester_id
    FROM sequenced
) src
PIVOT (
    MAX(prev2_requester_id)
    FOR channel IN ('web', 'phone')
)
ORDER BY agent_id
