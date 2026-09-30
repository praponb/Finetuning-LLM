WITH valid_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        requester_id - csat AS requester_id_minus_csat,
        ARRAY_CONTAINS(tags, 'login') AS has_login
    FROM tickets
    WHERE requester_id < 18
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.channel,
        s.csat,
        s.requester_id_minus_csat,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM valid_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY csat, YEAR(agent_hired_on)) AS rows_in_csat_agent_hired_on_year,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(agent_hired_on) ORDER BY agent_hired_on ASC, id), 3) AS agent_hired_on_cume_dist,
        SIZE(COLLECT_SET(agent_team) OVER w_csat_agent_hired_on_year) AS distinct_agent_team_in_csat_agent_hired_on_year,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY csat, YEAR(agent_hired_on)), 0), 4) AS csat_share_of_csat_agent_hired_on_year,
        LAST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY channel, DATE_TRUNC('MONTH', closed_at) ORDER BY csat NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at,
        NTILE(5) OVER w_channel_closed_at_month_by_c_2 AS csat_quintile,
        NTH_VALUE(closed_at, 2) OVER (ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        COUNT(*) OVER w_opened_at_month AS rows_in_opened_at_month,
        ROUND(PERCENT_RANK() OVER (PARTITION BY channel, agent_id ORDER BY closed_at), 3) AS closed_at_percent_rank,
        DENSE_RANK() OVER w_channel_agent_id_by_closed_a_2 AS closed_at_dense_rank,
        ROUND(csat - AVG(csat) OVER (PARTITION BY csat, YEAR(agent_hired_on)), 2) AS csat_vs_csat_agent_hired_on_year_avg,
        ROUND(AVG(csat) OVER (PARTITION BY channel, DATE_TRUNC('MONTH', closed_at) ORDER BY csat NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS csat_avg_7,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (PARTITION BY channel, agent_id ORDER BY closed_at, id))) / 60, 1) AS minutes_since_prev_opened_at
    FROM with_details
    WINDOW w_csat_agent_hired_on_year AS (PARTITION BY csat, YEAR(agent_hired_on)), w_channel_closed_at_month_by_c_2 AS (PARTITION BY channel, DATE_TRUNC('MONTH', closed_at) ORDER BY csat NULLS LAST, id), w_opened_at_month AS (PARTITION BY MONTH(opened_at)), w_channel_agent_id_by_closed_a_2 AS (PARTITION BY channel, agent_id ORDER BY closed_at, id)
),
with_trends_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_id,
        closed_at,
        csat,
        requester_id_minus_csat,
        agent_team,
        agent_hired_on,
        rows_in_csat_agent_hired_on_year,
        agent_hired_on_cume_dist,
        distinct_agent_team_in_csat_agent_hired_on_year,
        last_known_opened_at,
        csat_quintile,
        second_closed_at,
        rows_in_opened_at_month,
        closed_at_percent_rank,
        closed_at_dense_rank,
        csat_vs_csat_agent_hired_on_year_avg,
        csat_avg_7,
        minutes_since_prev_opened_at,
        FIRST_VALUE(minutes_since_prev_opened_at) IGNORE NULLS OVER (PARTITION BY csat ORDER BY requester_id_minus_csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_minutes_since_prev_opened_at,
        ROW_NUMBER() OVER (PARTITION BY csat ORDER BY requester_id_minus_csat, id) AS requester_id_minus_csat_seq,
        LAST_VALUE(requester_id_minus_csat) IGNORE NULLS OVER (PARTITION BY csat_quintile ORDER BY rows_in_csat_agent_hired_on_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_requester_id_minus_csat,
        ROUND(PERCENT_RANK() OVER (PARTITION BY csat ORDER BY requester_id_minus_csat), 3) AS requester_id_minus_csat_percent_rank,
        LAG(closed_at_dense_rank) OVER (PARTITION BY csat ORDER BY requester_id_minus_csat, id) AS prev_closed_at_dense_rank,
        NTILE(10) OVER (PARTITION BY csat ORDER BY requester_id_minus_csat, id) AS requester_id_minus_csat_decile
    FROM with_trends
)
-- Final projection
SELECT
    id,
    closed_at,
    csat,
    agent_team,
    agent_hired_on,
    rows_in_csat_agent_hired_on_year,
    distinct_agent_team_in_csat_agent_hired_on_year,
    last_known_opened_at,
    second_closed_at,
    rows_in_opened_at_month,
    closed_at_percent_rank,
    closed_at_dense_rank,
    csat_vs_csat_agent_hired_on_year_avg,
    csat_avg_7,
    minutes_since_prev_opened_at,
    first_known_minutes_since_prev_opened_at,
    requester_id_minus_csat_seq,
    last_known_requester_id_minus_csat,
    requester_id_minus_csat_percent_rank,
    requester_id_minus_csat_decile,
    DATE_TRUNC('HOUR', second_closed_at) AS second_closed_at_hour
FROM with_trends_2
ORDER BY closed_at_dense_rank DESC, second_closed_at_hour
