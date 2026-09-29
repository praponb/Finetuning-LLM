WITH base AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        csat,
        tags,
        requester_id - csat AS requester_id_minus_csat,
        LEAST(csat, requester_id) AS least_csat_requester_id
    FROM tickets
    WHERE priority = 'P3'
),
flattened AS (
    -- One row per tag
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat,
        s.requester_id_minus_csat,
        s.least_csat_requester_id,
        t.tag_pos,
        t.tag
    FROM base s
    LATERAL VIEW POSEXPLODE(s.tags) t AS tag_pos, tag
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        closed_at,
        csat,
        requester_id_minus_csat,
        least_csat_requester_id,
        tag_pos,
        tag,
        NTILE(10) OVER (PARTITION BY csat ORDER BY csat, id, tag_pos) AS csat_decile,
        COUNT(*) OVER (PARTITION BY agent_id, csat ORDER BY requester_id DESC, id, tag_pos ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM flattened
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        agent_id,
        closed_at,
        csat,
        requester_id_minus_csat,
        tag_pos,
        tag,
        csat_decile,
        running_count,
        DATE_TRUNC('MONTH', closed_at) AS closed_at_month
    FROM with_windows
    WHERE running_count < 18
),
latest_per_key AS (
    -- Keep the earliest row per tag, agent_id
    SELECT
        id,
        agent_id,
        closed_at,
        csat,
        requester_id_minus_csat,
        tag_pos,
        tag,
        csat_decile,
        running_count,
        closed_at_month
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tag, agent_id ORDER BY csat_decile, id, tag_pos) AS recency_rank
        FROM annotated
    ) ranked
    WHERE recency_rank = 1
),
tagged AS (
    -- Label and score rows
    SELECT *, LOWER(REGEXP_REPLACE(TRIM(tag), ' ', '_')) AS tag_slug
    FROM latest_per_key
),
agent_id_stats AS (
    -- Avg running_count per agent_id
    SELECT agent_id, AVG(running_count) AS avg_running_count
    FROM tagged
    GROUP BY agent_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_running_count,
        ROUND(s.running_count - g.avg_running_count, 2) AS running_count_minus_avg
    FROM tagged s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.running_count > g.avg_running_count
)
SELECT
    tag_pos,
    csat_decile,
    COUNT(*) AS row_count,
    ROUND(STDDEV(tag_pos), 2) AS stddev_tag_pos,
    MIN(closed_at_month) AS first_closed_at_month
FROM vs_group
GROUP BY ROLLUP(tag_pos, csat_decile)
ORDER BY tag_pos NULLS LAST, csat_decile NULLS LAST
