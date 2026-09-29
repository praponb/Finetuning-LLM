WITH base_tickets AS (
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
        HOUR(closed_at) AS closed_at_hour,
        DATE_TRUNC('WEEK', opened_at) AS opened_at_week
    FROM tickets
    WHERE agent_id >= 4
      AND priority <> 'P1'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        csat,
        tags,
        opened_at_week,
        ROUND(AVG(closed_at_hour) OVER (PARTITION BY agent_id, csat ORDER BY closed_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS closed_at_hour_avg_6_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY closed_at_hour, priority ORDER BY csat NULLS LAST), 3) AS csat_percent_rank,
        ROUND(PERCENT_RANK() OVER w_all_by_opened_at_week_ties, 3) AS opened_at_week_percent_rank,
        ROW_NUMBER() OVER (ORDER BY opened_at_week, id) AS opened_at_week_seq,
        ROUND(CUME_DIST() OVER w_closed_at_hour_priority_by_c, 3) AS csat_cume_dist,
        MIN(requester_id) OVER (PARTITION BY csat) AS csat_min_requester_id,
        LAST_VALUE(requester_id) OVER (ORDER BY opened_at_week, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_requester_id,
        ROUND(CUME_DIST() OVER (ORDER BY opened_at_week, id), 3) AS opened_at_week_cume_dist
    FROM base_tickets
    WINDOW w_all_by_opened_at_week_ties AS (ORDER BY opened_at_week), w_closed_at_hour_priority_by_c AS (PARTITION BY closed_at_hour, priority ORDER BY csat NULLS LAST)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        csat,
        closed_at_hour_avg_6_prior,
        opened_at_week_seq,
        csat_cume_dist,
        csat_min_requester_id,
        last_requester_id,
        opened_at_week_cume_dist,
        COUNT(*) OVER (ORDER BY opened_at_week, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at) ORDER BY opened_at_week_seq DESC, id), 3) AS opened_at_week_seq_cume_dist,
        COUNT(*) OVER (PARTITION BY agent_id) AS rows_in_agent_id,
        ROUND(AVG(opened_at_week_seq) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at) ORDER BY opened_at_week_seq DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS opened_at_week_seq_avg_2,
        ROUND(closed_at_hour_avg_6_prior / NULLIF(SUM(closed_at_hour_avg_6_prior) OVER w_agent_id, 0), 4) AS closed_at_hour_avg_6_prior_share_of_agent_id,
        COUNT(*) OVER (PARTITION BY opened_at_week_seq, YEAR(opened_at)) AS rows_in_opened_at_week_seq_opened_at_year,
        LEAD(opened_at_week_percent_rank, 3) OVER (ORDER BY opened_at_week, id) AS next3_opened_at_week_percent_rank
    FROM ranked
    WINDOW w_agent_id AS (PARTITION BY agent_id)
)
-- Final projection
SELECT
    id,
    requester_id,
    csat,
    closed_at_hour_avg_6_prior,
    opened_at_week_seq,
    csat_cume_dist,
    csat_min_requester_id,
    last_requester_id,
    opened_at_week_cume_dist,
    running_count,
    opened_at_week_seq_cume_dist,
    rows_in_agent_id,
    opened_at_week_seq_avg_2,
    rows_in_opened_at_week_seq_opened_at_year,
    next3_opened_at_week_percent_rank,
    ROUND(opened_at_week_seq / NULLIF(last_requester_id, 0), 3) AS opened_at_week_seq_per_last_requester_id,
    FLOOR(opened_at_week_seq / 2) * 2 AS opened_at_week_seq_bucket
FROM with_windows
ORDER BY closed_at_hour_avg_6_prior, requester_id
