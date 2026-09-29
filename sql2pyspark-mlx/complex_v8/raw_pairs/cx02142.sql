WITH scoped_tickets AS (
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
    WHERE priority NOT IN ('P2', 'P4')
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(csat) OVER (PARTITION BY MONTH(closed_at) ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_csat,
        SUM(csat) OVER (PARTITION BY MONTH(closed_at)) AS closed_at_month_sum_csat,
        ROUND(AVG(requester_id) OVER w_closed_at_month, 2) AS closed_at_month_avg_requester_id,
        csat - LAG(csat) OVER (ORDER BY closed_at DESC NULLS FIRST, id) AS csat_change,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER w_all_by_closed_at)) / 60, 1) AS minutes_since_prev_closed_at,
        FIRST_VALUE(csat) OVER (ORDER BY closed_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_csat,
        COUNT(*) OVER w_closed_at_month AS rows_in_closed_at_month,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY MONTH(closed_at)), 0), 4) AS requester_id_share_of_closed_at_month,
        MIN(requester_id) OVER (PARTITION BY MONTH(closed_at)) AS closed_at_month_min_requester_id,
        NTH_VALUE(closed_at, 2) OVER (ORDER BY closed_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        ROUND(CUME_DIST() OVER w_csat_by_opened_at, 3) AS opened_at_cume_dist
    FROM scoped_tickets
    WINDOW w_closed_at_month AS (PARTITION BY MONTH(closed_at)), w_all_by_closed_at AS (ORDER BY closed_at DESC NULLS FIRST, id), w_csat_by_opened_at AS (PARTITION BY csat ORDER BY opened_at, id)
),
derived AS (
    -- Derive helper columns
    SELECT *, SUBSTRING(TRIM(channel), 1, 4) AS channel_prefix
    FROM ranked
    WHERE first_csat BETWEEN 2 AND 4
)
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
    last_csat,
    closed_at_month_avg_requester_id,
    closed_at_month_min_requester_id,
    second_closed_at,
    channel_prefix,
    LOWER(TRIM(channel)) AS channel_clean
FROM derived
ORDER BY channel_prefix
