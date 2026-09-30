WITH valid AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        ARRAY_JOIN(ARRAY_SORT(tags), ',') AS tags_text,
        CASE WHEN requester_id >= 18 THEN 'HIGH' WHEN requester_id >= 7 THEN 'MEDIUM' ELSE 'LOW' END AS requester_id_tier,
        CASE WHEN channel IN ('email', 'phone') THEN 'group_a' WHEN channel = 'chat' THEN 'group_b' ELSE 'other' END AS channel_group
    FROM tickets
    WHERE SIZE(tags) >= 2
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS first_event_at,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS first_event_at_2
    FROM valid s
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(csat - AVG(csat) OVER (PARTITION BY TO_DATE(closed_at)), 2) AS csat_vs_closed_at_to_date_avg,
        ROW_NUMBER() OVER (PARTITION BY requester_id_tier, DATE_TRUNC('MONTH', closed_at) ORDER BY requester_id, id) AS requester_id_seq,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY TO_DATE(closed_at))) AS distinct_priority_in_closed_at_to_date,
        MAX(requester_id) OVER (PARTITION BY requester_id_tier, DATE_TRUNC('MONTH', closed_at) ORDER BY requester_id, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS requester_id_max_6_prior,
        LAG(first_event_at, 3) OVER (PARTITION BY requester_id_tier, DATE_TRUNC('MONTH', closed_at) ORDER BY requester_id, id) AS prev3_first_event_at,
        SUM(csat) OVER (PARTITION BY requester_id_tier, DATE_TRUNC('MONTH', closed_at) ORDER BY requester_id, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS csat_sum_2
    FROM summarized_rows
)
-- Subtotals with CUBE
SELECT
    csat,
    COALESCE(requester_id_tier, 'ALL') AS requester_id_tier,
    ARRAY_SORT(COLLECT_SET(channel)) AS channel_list,
    MIN(first_event_at) AS first_first_event_at
FROM with_trends
GROUP BY CUBE(csat, requester_id_tier)
ORDER BY csat NULLS LAST, requester_id_tier NULLS LAST
