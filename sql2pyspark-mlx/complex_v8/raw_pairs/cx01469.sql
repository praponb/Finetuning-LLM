WITH recent AS (
    -- Keep valid rows and derive helper columns
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
        SIZE(tags) AS tags_count,
        ARRAY_CONTAINS(tags, 'outage') AS has_outage,
        GREATEST(requester_id, csat) AS greatest_requester_id_csat
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-02-18 11:00:00'
),
joined AS (
    -- Attach agent attributes
    SELECT s.*, su.team AS agent_team, su.tier AS agent_tier
    FROM recent s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        GREATEST(tags_count, requester_id) AS greatest_tags_count_requester_id
    FROM joined
),
with_windows AS (
    -- Window metrics over several partitions
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
        tags_count,
        has_outage,
        greatest_requester_id_csat,
        agent_tier,
        greatest_tags_count_requester_id,
        ROUND(csat - AVG(csat) OVER w_csat, 2) AS csat_vs_csat_avg,
        FIRST_VALUE(tags_count) IGNORE NULLS OVER (PARTITION BY priority ORDER BY greatest_tags_count_requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_tags_count,
        ROUND(PERCENT_RANK() OVER (PARTITION BY priority ORDER BY greatest_tags_count_requester_id), 3) AS greatest_tags_count_requester_id_percent_rank,
        csat - LAG(csat) OVER w_priority_by_greatest_tags_co_3 AS csat_change,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY priority ORDER BY greatest_tags_count_requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at
    FROM tagged
    WINDOW w_csat AS (PARTITION BY csat), w_priority_by_greatest_tags_co_3 AS (PARTITION BY priority ORDER BY greatest_tags_count_requester_id, id)
),
exploded AS (
    -- One row per tag
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags_count,
        s.has_outage,
        s.greatest_requester_id_csat,
        s.agent_tier,
        s.greatest_tags_count_requester_id,
        s.csat_vs_csat_avg,
        s.first_known_tags_count,
        s.greatest_tags_count_requester_id_percent_rank,
        s.csat_change,
        s.second_closed_at,
        t.tag
    FROM with_windows s
    LATERAL VIEW EXPLODE(s.tags) t AS tag
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        priority,
        tags_count,
        has_outage,
        greatest_requester_id_csat,
        csat_vs_csat_avg,
        first_known_tags_count,
        csat_change,
        second_closed_at,
        tag,
        NTH_VALUE(greatest_requester_id_csat, 2) OVER (PARTITION BY priority, csat_change ORDER BY csat_vs_csat_avg DESC, id, tag ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_greatest_requester_id_csat,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY tag, first_known_tags_count)) AS distinct_agent_id_in_tag_first_known_tags_count
    FROM exploded
)
-- Final projection
SELECT
    id,
    requester_id,
    priority,
    has_outage,
    greatest_requester_id_csat,
    first_known_tags_count,
    csat_change,
    second_closed_at,
    tag,
    distinct_agent_id_in_tag_first_known_tags_count
FROM sequenced
WHERE greatest_requester_id_csat BETWEEN 3 AND 19
ORDER BY first_known_tags_count, distinct_agent_id_in_tag_first_known_tags_count
