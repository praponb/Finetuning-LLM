WITH valid_tickets AS (
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
        QUARTER(opened_at) AS opened_at_quarter,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug,
        QUARTER(closed_at) AS closed_at_quarter
    FROM tickets
    WHERE channel IN ('chat', 'web', 'email')
      AND opened_at >= TIMESTAMP '2025-01-25 11:00:00'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        opened_at,
        channel,
        csat,
        tags,
        ROUND(opened_at_quarter / NULLIF(SUM(opened_at_quarter) OVER (PARTITION BY channel), 0), 4) AS opened_at_quarter_share_of_channel,
        ROUND(PERCENT_RANK() OVER w_opened_at_year_by_requester, 3) AS requester_id_percent_rank,
        closed_at_quarter - LAG(closed_at_quarter) OVER (PARTITION BY YEAR(opened_at) ORDER BY requester_id ASC, id) AS closed_at_quarter_change,
        DENSE_RANK() OVER w_opened_at_year_by_requester AS requester_id_dense_rank,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (PARTITION BY YEAR(opened_at) ORDER BY requester_id ASC, id))) / 60, 1) AS minutes_since_prev_opened_at
    FROM valid_tickets
    WINDOW w_opened_at_year_by_requester AS (PARTITION BY YEAR(opened_at) ORDER BY requester_id ASC, id)
),
top_per_group AS (
    -- Top 3 per group
    SELECT
        id,
        requester_id,
        opened_at,
        channel,
        csat,
        tags,
        opened_at_quarter_share_of_channel,
        requester_id_percent_rank,
        closed_at_quarter_change,
        minutes_since_prev_opened_at
    FROM with_trends
    WHERE requester_id_dense_rank <= 3
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, reading
FROM (
    SELECT id, requester_id, closed_at_quarter_change
    FROM top_per_group
) src
UNPIVOT (
    reading FOR metric IN (requester_id, closed_at_quarter_change)
) unp
ORDER BY id, metric
