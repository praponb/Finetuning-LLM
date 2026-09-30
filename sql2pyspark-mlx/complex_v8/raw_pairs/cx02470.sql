WITH prepared AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        csat,
        DATE_FORMAT(opened_at, 'yyyy-MM-dd HH:00') AS opened_at_label,
        DATE_FORMAT(closed_at, 'MMM yyyy') AS closed_at_label,
        DATEDIFF(DATE '2025-06-30', opened_at) AS days_since_opened_at
    FROM tickets
    WHERE csat < 4
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        csat,
        opened_at_label,
        closed_at_label,
        CASE WHEN priority IN ('P2', 'P1') THEN 'group_a' WHEN priority = 'P3' THEN 'group_b' ELSE 'other' END AS priority_group
    FROM prepared
    WHERE opened_at_label IS NOT NULL
),
priority_stats AS (
    -- Median requester_id per priority
    SELECT priority, PERCENTILE_APPROX(requester_id, 0.5) AS median_requester_id
    FROM enriched
    GROUP BY priority
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_requester_id,
        ROUND(s.requester_id - g.median_requester_id, 2) AS requester_id_minus_median
    FROM enriched s
    JOIN priority_stats g ON g.priority = s.priority
    WHERE s.requester_id >= g.median_requester_id
),
filtered AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM vs_group s
    WHERE s.id IN (
            SELECT t.ticket_id
            FROM ticket_events t
        )
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        agent_id,
        priority,
        closed_at_label,
        median_requester_id,
        requester_id_minus_median,
        CONCAT(opened_at_label, '#', CAST(agent_id AS STRING)) AS opened_at_label_ref
    FROM filtered
    WHERE opened_at_label IS NOT NULL
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        agent_id,
        priority,
        closed_at_label,
        median_requester_id,
        requester_id_minus_median,
        opened_at_label_ref,
        LOWER(REGEXP_REPLACE(TRIM(closed_at_label), ' ', '_')) AS closed_at_label_slug,
        FLOOR(median_requester_id / 5) * 5 AS median_requester_id_bucket,
        CONCAT_WS(': ', closed_at_label, priority) AS closed_at_label_priority_label
    FROM scored
),
combined_details AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.median_requester_id,
        s.opened_at_label_ref,
        s.closed_at_label_slug,
        s.median_requester_id_bucket,
        s.closed_at_label_priority_label,
        su.agent_name,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on
    FROM derived s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE UPPER(TRIM(su.agent_name)) LIKE 'G%'
)
SELECT
    id,
    agent_id,
    priority,
    opened_at_label_ref,
    closed_at_label_slug,
    median_requester_id_bucket,
    agent_name,
    agent_tier,
    DATE_FORMAT(agent_hired_on, 'yyyy-MM') AS agent_hired_on_label
FROM combined_details
WHERE agent_name IS NOT NULL
ORDER BY agent_name DESC
