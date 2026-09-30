WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        CONCAT(channel, '#', CAST(id AS STRING)) AS channel_ref
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-01-26 18:00:00'
),
combined_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel_ref,
        su.agent_name,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM filtered s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel_ref,
        agent_name,
        LAST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY YEAR(opened_at) ORDER BY opened_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_closed_at,
        RANK() OVER w_agent_hired_on_year_by_close AS closed_at_rank,
        ROW_NUMBER() OVER (PARTITION BY YEAR(closed_at) ORDER BY closed_at DESC, id) AS seq_no,
        LAG(opened_at, 3) OVER (ORDER BY agent_hired_on DESC, id) AS prev3_opened_at,
        LAG(agent_hired_on) OVER (PARTITION BY YEAR(agent_hired_on) ORDER BY closed_at NULLS LAST, id) AS prev_agent_hired_on,
        FIRST_VALUE(agent_hired_on) IGNORE NULLS OVER (ORDER BY agent_hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_agent_hired_on,
        FIRST_VALUE(agent_hired_on) IGNORE NULLS OVER (PARTITION BY YEAR(opened_at) ORDER BY opened_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_agent_hired_on_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(opened_at) ORDER BY opened_at ASC NULLS LAST), 3) AS opened_at_percent_rank,
        FIRST_VALUE(opened_at) OVER (PARTITION BY priority, agent_team ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_opened_at,
        DENSE_RANK() OVER (PARTITION BY YEAR(agent_hired_on) ORDER BY closed_at NULLS LAST, id) AS closed_at_dense_rank
    FROM combined_details_agent
    WINDOW w_agent_hired_on_year_by_close AS (PARTITION BY YEAR(agent_hired_on) ORDER BY closed_at NULLS LAST, id)
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        closed_at,
        priority,
        channel_ref,
        closed_at_rank,
        seq_no,
        prev3_opened_at,
        prev_agent_hired_on,
        first_known_agent_hired_on,
        first_known_agent_hired_on_2,
        first_opened_at,
        closed_at_dense_rank,
        CONCAT_WS(': ', channel_ref, agent_name) AS channel_ref_agent_name_label
    FROM windowed
),
windowed_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY closed_at_dense_rank, closed_at_rank ORDER BY first_opened_at DESC, id) AS seq_no_2,
        RANK() OVER (PARTITION BY MONTH(first_opened_at) ORDER BY prev_agent_hired_on, id) AS prev_agent_hired_on_rank,
        ROUND(closed_at_rank / NULLIF(SUM(closed_at_rank) OVER (PARTITION BY DATE_TRUNC('MONTH', first_known_agent_hired_on)), 0), 4) AS closed_at_rank_share_of_first_known_agent_hired_,
        ROUND(AVG(closed_at_rank) OVER (PARTITION BY closed_at_dense_rank, closed_at_rank ORDER BY first_opened_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS closed_at_rank_avg_2_prior,
        NTILE(10) OVER (PARTITION BY MONTH(first_opened_at) ORDER BY prev_agent_hired_on, id) AS prev_agent_hired_on_decile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(first_opened_at) ORDER BY prev_agent_hired_on, id), 3) AS prev_agent_hired_on_percent_rank
    FROM tagged
)
SELECT
    id,
    priority,
    channel_ref,
    prev3_opened_at,
    prev_agent_hired_on,
    first_known_agent_hired_on,
    first_known_agent_hired_on_2,
    first_opened_at,
    channel_ref_agent_name_label,
    seq_no_2,
    closed_at_rank_share_of_first_known_agent_hired_,
    closed_at_rank_avg_2_prior,
    prev_agent_hired_on_decile,
    prev_agent_hired_on_percent_rank
FROM windowed_2
ORDER BY seq_no_2 DESC NULLS FIRST, prev3_opened_at
