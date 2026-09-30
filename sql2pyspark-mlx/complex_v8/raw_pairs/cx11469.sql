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
        tags,
        COALESCE(csat, 0) AS csat_filled,
        SUBSTRING(TRIM(channel), 1, 4) AS channel_prefix,
        CONCAT(priority, '#', CAST(requester_id AS STRING)) AS priority_ref
    FROM tickets
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY requester_id ASC) AS requester_id_dense_rank,
        FIRST_VALUE(csat_filled) OVER (ORDER BY csat_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_csat_filled,
        SUM(csat) OVER (ORDER BY csat_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER w_csat_opened_at_month, 0), 4) AS requester_id_share_of_csat_opened_at_month,
        ROUND(CUME_DIST() OVER w_all_by_requester_id, 3) AS requester_id_cume_dist
    FROM valid
    WINDOW w_csat_opened_at_month AS (PARTITION BY csat, MONTH(opened_at)), w_all_by_requester_id AS (ORDER BY requester_id ASC, id)
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.channel,
        s.csat_filled,
        s.channel_prefix,
        s.first_csat_filled,
        s.requester_id_cume_dist,
        t.id AS event_id,
        t.event_type,
        t.actor AS event_actor,
        t.event_at
    FROM windowed s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
),
summary AS (
    -- Group-level totals
    SELECT
        priority,
        ROUND(SUM(requester_id_cume_dist), 2) AS total_requester_id_cume_dist,
        ARRAY_SORT(COLLECT_SET(priority)) AS priority_list,
        COUNT(DISTINCT event_type) AS distinct_event_types
    FROM exploded_detail
    GROUP BY priority
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY distinct_event_types)) AS distinct_priority_in_distinct_event_types,
        ROUND(distinct_event_types / NULLIF(SUM(distinct_event_types) OVER w_distinct_event_types, 0), 4) AS distinct_event_types_share_of_distinct_event_typ,
        NTH_VALUE(total_requester_id_cume_dist, 2) OVER (ORDER BY distinct_event_types DESC, priority ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_total_requester_id_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY priority ORDER BY total_requester_id_cume_dist, priority), 3) AS total_requester_id_cume_dist_cume_dist
    FROM summary
    WINDOW w_distinct_event_types AS (PARTITION BY distinct_event_types)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN priority IN ('P4', 'P2') THEN 'group_a' WHEN priority = 'P1' THEN 'group_b' ELSE 'other' END AS priority_group,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug
    FROM with_windows
    WHERE second_total_requester_id_cume_dist >= 1.5
)
SELECT
    priority,
    total_requester_id_cume_dist,
    priority_list,
    distinct_priority_in_distinct_event_types,
    distinct_event_types_share_of_distinct_event_typ,
    second_total_requester_id_cume_dist,
    total_requester_id_cume_dist_cume_dist,
    priority_group,
    priority_slug,
    CONCAT_WS('-', priority_group, priority) AS priority_group_priority_label,
    FLOOR(distinct_event_types / 2) * 2 AS distinct_event_types_bucket
FROM annotated
