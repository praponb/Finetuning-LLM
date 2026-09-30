WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.csat) AS prior_csat_1d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_id,
        opened_at,
        prior_rows_1d,
        prior_csat_1d,
        ROUND(AVG(prior_rows_1d) OVER (PARTITION BY prior_rows_1d, agent_id), 2) AS prior_rows_1d_agent_id_avg_prior_rows_1d,
        ROUND(CUME_DIST() OVER w_prior_rows_1d_agent_id_by_pr, 3) AS prior_rows_1d_cume_dist
    FROM matched
    WINDOW w_prior_rows_1d_agent_id_by_pr AS (PARTITION BY prior_rows_1d, agent_id ORDER BY prior_rows_1d ASC)
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(prior_csat_1d / NULLIF(prior_rows_1d_cume_dist, 0), 3) AS prior_csat_1d_per_prior_rows_1d_cume_dist,
        COALESCE(prior_rows_1d, 0) AS prior_rows_1d_filled
    FROM with_trends
    WHERE agent_id IS NOT NULL
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, COUNT(*) AS event_count, MIN(event_at) AS first_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count,
        t.first_event_at
    FROM enriched s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
)
SELECT
    agent_id,
    event_count,
    prior_rows_1d_filled,
    GROUPING_ID() AS gid,
    ROUND(AVG(prior_csat_1d_per_prior_rows_1d_cume_dist), 2) AS avg_prior_csat_1d_per_prior_rows_1d_cume_dist,
    PERCENTILE_APPROX(prior_rows_1d_cume_dist, 0.5) AS median_prior_rows_1d_cume_dist
FROM profiled
GROUP BY GROUPING SETS ((agent_id, event_count, prior_rows_1d_filled), (agent_id), ())
