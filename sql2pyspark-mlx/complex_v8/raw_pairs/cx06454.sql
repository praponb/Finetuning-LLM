WITH base AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        INITCAP(TRIM(channel)) AS channel_clean,
        SUBSTRING(TRIM(channel), 1, 2) AS channel_prefix
    FROM tickets
),
with_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.csat,
        s.tags,
        s.channel_clean,
        s.channel_prefix,
        su.agent_name
    FROM base s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        opened_at,
        csat,
        tags,
        channel_clean,
        agent_name,
        DENSE_RANK() OVER (ORDER BY closed_at DESC) AS closed_at_dense_rank,
        ROUND(AVG(csat) OVER w_csat_opened_at_month, 2) AS csat_opened_at_month_avg_csat,
        LAG(opened_at, 3) OVER (PARTITION BY csat ORDER BY opened_at DESC NULLS FIRST, id) AS prev3_opened_at,
        ROUND(CUME_DIST() OVER w_agent_id_csat_by_csat_ties, 3) AS csat_cume_dist,
        MIN(csat) OVER (PARTITION BY MONTH(closed_at), agent_id) AS closed_at_month_agent_id_min_csat,
        COUNT(*) OVER (PARTITION BY MONTH(closed_at), agent_id) AS rows_in_closed_at_month_agent_id,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER w_all_by_closed_at)) / 60, 1) AS minutes_since_prev_opened_at,
        MAX(csat) OVER (PARTITION BY MONTH(closed_at), agent_id) AS closed_at_month_agent_id_max_csat,
        ROUND(PERCENT_RANK() OVER (PARTITION BY csat ORDER BY opened_at DESC NULLS FIRST), 3) AS opened_at_percent_rank,
        ROUND(csat - AVG(csat) OVER (PARTITION BY csat, MONTH(opened_at)), 2) AS csat_vs_csat_opened_at_month_avg,
        RANK() OVER (PARTITION BY agent_id ORDER BY csat DESC) AS csat_rank,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY csat, MONTH(opened_at)), 0), 4) AS csat_share_of_csat_opened_at_month,
        MIN(csat) OVER (PARTITION BY csat, MONTH(opened_at)) AS csat_opened_at_month_min_csat,
        LAG(csat) OVER w_all_by_closed_at AS prev_csat
    FROM with_details_agent
    WINDOW w_csat_opened_at_month AS (PARTITION BY csat, MONTH(opened_at)), w_agent_id_csat_by_csat_ties AS (PARTITION BY agent_id, csat ORDER BY csat DESC), w_all_by_closed_at AS (ORDER BY closed_at DESC, id)
),
annotated AS (
    -- Derive helper columns
    SELECT *, SUBSTRING(TRIM(agent_name), 1, 2) AS agent_name_prefix
    FROM with_windows
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        opened_at,
        channel_clean,
        closed_at_dense_rank,
        csat_opened_at_month_avg_csat,
        closed_at_month_agent_id_min_csat,
        rows_in_closed_at_month_agent_id,
        opened_at_percent_rank,
        csat_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY closed_at_month_agent_id_max_csat DESC NULLS FIRST), 3) AS closed_at_month_agent_id_max_csat_percent_rank,
        ROUND(AVG(csat) OVER (PARTITION BY prev_csat ORDER BY csat_share_of_csat_opened_at_month, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_2_prior,
        ROUND(opened_at_percent_rank / NULLIF(SUM(opened_at_percent_rank) OVER (PARTITION BY prev_csat, agent_id), 0), 4) AS opened_at_percent_rank_share_of_prev_csat_agent_,
        ROW_NUMBER() OVER w_prev_csat_by_csat_share_of_c_2 AS seq_no
    FROM annotated
    WINDOW w_prev_csat_by_csat_share_of_c_2 AS (PARTITION BY prev_csat ORDER BY csat_share_of_csat_opened_at_month, id)
)
SELECT
    id,
    agent_id,
    opened_at,
    channel_clean,
    closed_at_dense_rank,
    csat_opened_at_month_avg_csat,
    closed_at_month_agent_id_min_csat,
    rows_in_closed_at_month_agent_id,
    opened_at_percent_rank,
    csat_rank,
    closed_at_month_agent_id_max_csat_percent_rank,
    opened_at_percent_rank_share_of_prev_csat_agent_,
    seq_no
FROM with_trends
WHERE csat_opened_at_month_avg_csat BETWEEN 1.4 AND 4.6
ORDER BY opened_at DESC
