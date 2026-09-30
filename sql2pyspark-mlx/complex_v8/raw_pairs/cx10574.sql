WITH valid AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE priority <> 'P4'
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM valid
    WHERE csat > (SELECT MIN(csat) FROM valid)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        ROUND(csat - AVG(csat) OVER (PARTITION BY agent_id, channel), 2) AS csat_vs_agent_id_channel_avg,
        SUM(csat) OVER (PARTITION BY agent_id, channel) AS agent_id_channel_sum_csat,
        SUM(csat) OVER (PARTITION BY channel, YEAR(opened_at) ORDER BY csat, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS csat_sum_3_prior
    FROM without_matches
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, COUNT(*) AS ticket_count, COUNT(*) AS ticket_count_2
    FROM tickets
    GROUP BY agent_id
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        COALESCE(t.ticket_count_2, 0) AS ticket_count_2
    FROM with_windows s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(priority, '#', CAST(csat AS STRING)) AS priority_ref,
        DATE_TRUNC('WEEK', opened_at) AS opened_at_week
    FROM summarized_rows
    WHERE ticket_count < 7
)
SELECT
    id,
    priority,
    channel,
    csat_vs_agent_id_channel_avg,
    csat_sum_3_prior,
    ticket_count,
    ticket_count_2,
    LENGTH(TRIM(priority)) AS priority_len
FROM enriched
ORDER BY csat_sum_3_prior NULLS LAST, priority DESC
