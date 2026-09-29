WITH cleaned AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags,
        DATE_TRUNC('DAY', closed_at) AS closed_at_day
    FROM tickets
    WHERE csat >= 3
      AND priority <> 'P2'
),
exploded AS (
    -- One row per tag
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.channel,
        s.closed_at_day,
        t.tag
    FROM cleaned s
    LATERAL VIEW EXPLODE(s.tags) t AS tag
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        opened_at,
        closed_at,
        closed_at_day,
        tag,
        ROUND(CUME_DIST() OVER (ORDER BY closed_at NULLS LAST), 3) AS closed_at_cume_dist,
        COUNT(*) OVER (PARTITION BY channel) AS rows_in_channel,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY channel), 0), 4) AS requester_id_share_of_channel,
        FIRST_VALUE(closed_at_day) IGNORE NULLS OVER (ORDER BY closed_at NULLS LAST, id, tag ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_closed_at_day,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY channel)) AS distinct_agent_id_in_channel,
        ROW_NUMBER() OVER (ORDER BY requester_id DESC, id, tag) AS rn
    FROM exploded
),
ranked_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(closed_at_cume_dist / NULLIF(SUM(closed_at_cume_dist) OVER (PARTITION BY tag), 0), 4) AS closed_at_cume_dist_share_of_tag,
        NTILE(4) OVER (PARTITION BY distinct_agent_id_in_channel ORDER BY requester_id, id, tag) AS requester_id_quartile,
        COUNT(*) OVER (PARTITION BY tag) AS rows_in_tag,
        ROUND(rn - AVG(rn) OVER (PARTITION BY tag), 2) AS rn_vs_tag_avg,
        MIN(rn) OVER (PARTITION BY rn, tag ORDER BY closed_at_cume_dist DESC, id, tag ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_rn_so_far
    FROM ranked
)
SELECT
    id,
    closed_at,
    tag,
    closed_at_cume_dist,
    rows_in_channel,
    requester_id_share_of_channel,
    rn,
    requester_id_quartile,
    min_rn_so_far,
    ABS(rn_vs_tag_avg - 3.0) AS rn_vs_tag_avg_distance
FROM ranked_2
ORDER BY closed_at_cume_dist DESC, requester_id_quartile DESC
