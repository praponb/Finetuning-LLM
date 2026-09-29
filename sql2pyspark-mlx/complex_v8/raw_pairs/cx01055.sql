WITH scoped AS (
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
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 3600, 1) AS hours_closed_at_to_opened_at,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at
    FROM tickets
    WHERE requester_id <= 18
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(opened_at) OVER (PARTITION BY csat, agent_id ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_opened_at,
        LAG(opened_at, 3) OVER (PARTITION BY csat, agent_id ORDER BY closed_at DESC, id) AS prev3_opened_at,
        NTILE(3) OVER w_all_by_minutes_opened_at_to AS minutes_opened_at_to_closed_at_tercile,
        RANK() OVER w_all_by_minutes_opened_at_to_2 AS minutes_opened_at_to_closed_at_rank
    FROM scoped
    WINDOW w_all_by_minutes_opened_at_to AS (ORDER BY minutes_opened_at_to_closed_at DESC, id), w_all_by_minutes_opened_at_to_2 AS (ORDER BY minutes_opened_at_to_closed_at DESC)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags,
        hours_closed_at_to_opened_at,
        minutes_opened_at_to_closed_at,
        last_opened_at,
        prev3_opened_at,
        minutes_opened_at_to_closed_at_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY opened_at, id), 3) AS opened_at_percent_rank,
        COUNT(*) OVER (PARTITION BY priority) AS rows_in_priority,
        MAX(requester_id) OVER w_priority AS priority_max_requester_id,
        MIN(hours_closed_at_to_opened_at) OVER (PARTITION BY priority) AS priority_min_hours_closed_at_to_opened_at
    FROM with_windows
    WINDOW w_priority AS (PARTITION BY priority)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(priority_max_requester_id) OVER (PARTITION BY csat ORDER BY minutes_opened_at_to_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_priority_max_requester_id,
        LAG(prev3_opened_at) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), agent_id ORDER BY opened_at_percent_rank, id) AS prev_prev3_opened_at,
        ROUND(CUME_DIST() OVER w_priority_csat_by_rows_in_pri, 3) AS rows_in_priority_cume_dist,
        MAX(hours_closed_at_to_opened_at) OVER (PARTITION BY DATE_TRUNC('MONTH', last_opened_at)) AS last_opened_at_month_max_hours_closed_at_to_open
    FROM windowed
    WINDOW w_priority_csat_by_rows_in_pri AS (PARTITION BY priority, csat ORDER BY rows_in_priority, id)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        opened_at,
        priority,
        last_opened_at,
        opened_at_percent_rank,
        rows_in_priority,
        priority_max_requester_id,
        last_priority_max_requester_id,
        prev_prev3_opened_at,
        rows_in_priority_cume_dist,
        ROUND(opened_at_percent_rank - AVG(opened_at_percent_rank) OVER (PARTITION BY minutes_opened_at_to_closed_at_rank), 2) AS opened_at_percent_rank_vs_minutes_opened_at_to_c,
        LAST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY YEAR(last_opened_at) ORDER BY last_opened_at_month_max_hours_closed_at_to_open, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at,
        MAX(priority_max_requester_id) OVER (PARTITION BY YEAR(last_opened_at) ORDER BY last_opened_at_month_max_hours_closed_at_to_open, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_priority_max_requester_id_so_far,
        ROUND(rows_in_priority - AVG(rows_in_priority) OVER w_minutes_opened_at_to_closed, 2) AS rows_in_priority_vs_minutes_opened_at_to_closed_
    FROM with_trends
    WINDOW w_minutes_opened_at_to_closed AS (PARTITION BY minutes_opened_at_to_closed_at_rank)
)
SELECT id, measure, reading
FROM (
    SELECT id, opened_at_percent_rank_vs_minutes_opened_at_to_c, rows_in_priority_cume_dist, opened_at_percent_rank
    FROM sequenced
) src
UNPIVOT (
    reading FOR measure IN (opened_at_percent_rank_vs_minutes_opened_at_to_c, rows_in_priority_cume_dist, opened_at_percent_rank)
) unp
ORDER BY id, measure
