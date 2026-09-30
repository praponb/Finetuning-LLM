WITH filtered_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 3600, 1) AS hours_opened_at_to_closed_at
    FROM tickets
    WHERE requester_id < 23
      AND agent_id IS NOT NULL
),
flattened AS (
    -- One row per tag
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.hours_opened_at_to_closed_at,
        t.tag
    FROM filtered_tickets s
    LATERAL VIEW EXPLODE(s.tags) t AS tag
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(hours_opened_at_to_closed_at) OVER (PARTITION BY agent_id, MONTH(closed_at)) AS agent_id_closed_at_month_max_hours_opened_at_to_,
        LAST_VALUE(hours_opened_at_to_closed_at) OVER (PARTITION BY tag ORDER BY closed_at DESC NULLS FIRST, id, tag ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_hours_opened_at_to_closed_at,
        ROUND(hours_opened_at_to_closed_at / NULLIF(SUM(hours_opened_at_to_closed_at) OVER (PARTITION BY agent_id, MONTH(closed_at)), 0), 4) AS hours_opened_at_to_closed_at_share_of_agent_id_c,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY agent_id, MONTH(closed_at)), 0), 4) AS csat_share_of_agent_id_closed_at_month,
        DENSE_RANK() OVER (PARTITION BY tag ORDER BY closed_at DESC NULLS FIRST) AS closed_at_dense_rank,
        FIRST_VALUE(csat) OVER (PARTITION BY tag ORDER BY closed_at DESC NULLS FIRST, id, tag ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_csat
    FROM flattened
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        hours_opened_at_to_closed_at,
        tag,
        agent_id_closed_at_month_max_hours_opened_at_to_,
        last_hours_opened_at_to_closed_at,
        hours_opened_at_to_closed_at_share_of_agent_id_c,
        csat_share_of_agent_id_closed_at_month,
        first_csat
    FROM metrics
    WHERE closed_at_dense_rank = 1
      AND first_csat > 2
),
latest_per_key AS (
    -- Keep the latest row per channel, agent_id
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        hours_opened_at_to_closed_at,
        tag,
        agent_id_closed_at_month_max_hours_opened_at_to_,
        last_hours_opened_at_to_closed_at,
        hours_opened_at_to_closed_at_share_of_agent_id_c,
        csat_share_of_agent_id_closed_at_month,
        first_csat
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY channel, agent_id ORDER BY closed_at DESC, id DESC, tag DESC) AS rn
        FROM top_rows
    ) ranked
    WHERE rn = 1
),
annotated AS (
    -- Add calculated columns
    SELECT *, LENGTH(TRIM(channel)) AS channel_len
    FROM latest_per_key
    WHERE csat_share_of_agent_id_closed_at_month <= 0.8
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(hours_opened_at_to_closed_at) OVER (PARTITION BY priority, first_csat ORDER BY agent_id_closed_at_month_max_hours_opened_at_to_, id, tag ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS hours_opened_at_to_closed_at_avg_2_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY priority, first_csat ORDER BY agent_id_closed_at_month_max_hours_opened_at_to_), 3) AS agent_id_closed_at_month_max_hours_opened_at_to__2,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY priority, first_csat ORDER BY agent_id_closed_at_month_max_hours_opened_at_to_, id, tag ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        NTILE(10) OVER (PARTITION BY priority, first_csat ORDER BY agent_id_closed_at_month_max_hours_opened_at_to_, id, tag) AS agent_id_closed_at_month_max_hours_opened_at_to__3,
        DENSE_RANK() OVER (PARTITION BY priority, first_csat ORDER BY agent_id_closed_at_month_max_hours_opened_at_to_) AS agent_id_closed_at_month_max_hours_opened_at_to__4,
        LAG(channel_len, 3, 0) OVER (PARTITION BY priority, first_csat ORDER BY agent_id_closed_at_month_max_hours_opened_at_to_, id, tag) AS prev3_channel_len
    FROM annotated
)
-- Subtotals with ROLLUP
SELECT
    first_csat,
    agent_id,
    ARRAY_SORT(COLLECT_SET(channel)) AS channel_list,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT priority) AS distinct_prioritys
FROM ranked
GROUP BY ROLLUP(first_csat, agent_id)
