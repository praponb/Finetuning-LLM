WITH valid_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        priority,
        channel,
        tags,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at
    FROM tickets
    WHERE csat <= 3
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER w_agent_id_by_minutes_opened_a AS minutes_opened_at_to_closed_at_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_id ORDER BY minutes_opened_at_to_closed_at), 3) AS minutes_opened_at_to_closed_at_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY channel ORDER BY minutes_opened_at_to_closed_at DESC NULLS FIRST), 3) AS minutes_opened_at_to_closed_at_cume_dist_2,
        NTILE(3) OVER w_agent_id_by_minutes_opened_a AS minutes_opened_at_to_closed_at_tercile,
        ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY minutes_opened_at_to_closed_at, id) AS seq_no,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_id, channel ORDER BY minutes_opened_at_to_closed_at DESC NULLS FIRST), 3) AS minutes_opened_at_to_closed_at_percent_rank,
        RANK() OVER (PARTITION BY channel ORDER BY minutes_opened_at_to_closed_at DESC NULLS FIRST) AS minutes_opened_at_to_closed_at_rank_2,
        ROW_NUMBER() OVER (PARTITION BY agent_id, channel ORDER BY minutes_opened_at_to_closed_at DESC NULLS FIRST, id) AS minutes_opened_at_to_closed_at_seq
    FROM valid_tickets
    WINDOW w_agent_id_by_minutes_opened_a AS (PARTITION BY agent_id ORDER BY minutes_opened_at_to_closed_at, id)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM ranked
    WHERE minutes_opened_at_to_closed_at_rank = 1
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        minutes_opened_at_to_closed_at_rank_2,
        agent_id,
        ROUND(SUM(minutes_opened_at_to_closed_at_tercile) / NULLIF(SUM(minutes_opened_at_to_closed_at_rank_2), 0), 3) AS minutes_opened_at_to_closed_at_tercile_to_minute,
        SUM(CASE WHEN channel IS NOT NULL THEN 1 ELSE 0 END) AS n_channel_is_not_null
    FROM top_rows
    GROUP BY minutes_opened_at_to_closed_at_rank_2, agent_id
    HAVING MAX(minutes_opened_at_to_closed_at_seq) > 2
)
SELECT
    minutes_opened_at_to_closed_at_rank_2,
    minutes_opened_at_to_closed_at_tercile_to_minute,
    n_channel_is_not_null,
    COALESCE(n_channel_is_not_null, 0) AS n_channel_is_not_null_filled,
    ABS(n_channel_is_not_null - 3) AS n_channel_is_not_null_distance
FROM stats
ORDER BY n_channel_is_not_null_distance DESC, minutes_opened_at_to_closed_at_tercile_to_minute DESC
