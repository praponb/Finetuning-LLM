WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.csat) AS prior_csat_12h
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.agent_id, a.opened_at
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at
    FROM joined_ranges s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        prior_rows_12h + prior_csat_12h AS prior_rows_12h_plus_prior_csat_12h,
        YEAR(opened_at) AS opened_at_year
    FROM summarized_rows
    WHERE HOUR(last_event_at) BETWEEN 7 AND 11
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(prior_rows_12h) OVER (PARTITION BY YEAR(last_event_at)), 2) AS last_event_at_year_avg_prior_rows_12h,
        ROUND(CUME_DIST() OVER (PARTITION BY prior_rows_12h ORDER BY opened_at ASC), 3) AS opened_at_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY prior_rows_12h ORDER BY opened_at ASC, id), 3) AS opened_at_percent_rank
    FROM enriched
)
SELECT id, metric, amount
FROM (
    SELECT id, opened_at_year, prior_csat_12h, prior_rows_12h_plus_prior_csat_12h, prior_rows_12h
    FROM with_trends
) src
UNPIVOT (
    amount FOR metric IN (opened_at_year, prior_csat_12h, prior_rows_12h_plus_prior_csat_12h, prior_rows_12h)
) unp
