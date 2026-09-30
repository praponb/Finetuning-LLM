WITH cleaned_tickets AS (
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
        tags
    FROM tickets
    WHERE csat >= 3
),
flattened AS (
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
        t.tag_pos,
        t.tag
    FROM cleaned_tickets s
    LATERAL VIEW POSEXPLODE(s.tags) t AS tag_pos, tag
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        closed_at,
        priority,
        csat,
        tag_pos,
        tag,
        FIRST_VALUE(requester_id) OVER (ORDER BY opened_at DESC NULLS FIRST, id, tag_pos ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_requester_id,
        ROUND(AVG(tag_pos) OVER (PARTITION BY priority, tag_pos ORDER BY closed_at DESC NULLS FIRST, id, tag_pos ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS tag_pos_avg_7,
        MAX(requester_id) OVER (ORDER BY opened_at DESC NULLS FIRST, id, tag_pos ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS requester_id_max_7,
        COUNT(*) OVER w_priority_tag_pos AS rows_in_priority_tag_pos
    FROM flattened
    WINDOW w_priority_tag_pos AS (PARTITION BY priority, tag_pos)
),
priority_stats AS (
    -- Median rows_in_priority_tag_pos per priority
    SELECT priority, PERCENTILE_APPROX(rows_in_priority_tag_pos, 0.5) AS median_rows_in_priority_tag_pos
    FROM sequenced
    GROUP BY priority
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_rows_in_priority_tag_pos,
        ROUND(s.rows_in_priority_tag_pos - g.median_rows_in_priority_tag_pos, 2) AS rows_in_priority_tag_pos_minus_median
    FROM sequenced s
    JOIN priority_stats g ON g.priority = s.priority
    WHERE s.rows_in_priority_tag_pos >= g.median_rows_in_priority_tag_pos
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(requester_id_max_7, 2) OVER (ORDER BY requester_id ASC NULLS LAST, id, tag_pos ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id_max_7,
        ROUND(CUME_DIST() OVER (ORDER BY median_rows_in_priority_tag_pos), 3) AS median_rows_in_priority_tag_pos_cume_dist
    FROM vs_group
),
priority_stats_2 AS (
    -- Avg rows_in_priority_tag_pos per priority
    SELECT priority, AVG(rows_in_priority_tag_pos) AS avg_rows_in_priority_tag_pos
    FROM ranked
    GROUP BY priority
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_rows_in_priority_tag_pos,
        ROUND(s.rows_in_priority_tag_pos - g.avg_rows_in_priority_tag_pos, 2) AS rows_in_priority_tag_pos_minus_avg
    FROM ranked s
    JOIN priority_stats_2 g ON g.priority = s.priority
    WHERE s.rows_in_priority_tag_pos > g.avg_rows_in_priority_tag_pos
)
SELECT
    id,
    requester_id,
    closed_at,
    priority,
    tag_pos,
    tag,
    tag_pos_avg_7,
    requester_id_max_7,
    rows_in_priority_tag_pos,
    median_rows_in_priority_tag_pos,
    rows_in_priority_tag_pos_minus_median,
    second_requester_id_max_7,
    median_rows_in_priority_tag_pos_cume_dist,
    avg_rows_in_priority_tag_pos,
    rows_in_priority_tag_pos_minus_avg
FROM outliers
