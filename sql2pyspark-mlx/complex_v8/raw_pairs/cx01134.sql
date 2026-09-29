WITH base AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat
    FROM tickets
    WHERE HOUR(opened_at) BETWEEN 10 AND 16
      AND csat <= 3
),
enriched_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.channel,
        s.csat,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM base s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(channel) OVER w_closed_at_month) AS distinct_channel_in_closed_at_month,
        RANK() OVER (PARTITION BY channel, csat ORDER BY closed_at, id) AS closed_at_rank,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (PARTITION BY channel, csat ORDER BY closed_at, id))) / 60, 1) AS minutes_since_prev_opened_at,
        MIN(requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at)) AS closed_at_month_min_requester_id,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY csat, TO_DATE(closed_at) ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        RANK() OVER (PARTITION BY csat, TO_DATE(closed_at) ORDER BY requester_id DESC) AS requester_id_rank,
        LEAD(csat, 2, 0) OVER (PARTITION BY channel, agent_team ORDER BY csat NULLS LAST, id) AS next2_csat,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at)) AS rows_in_closed_at_month,
        FIRST_VALUE(requester_id) IGNORE NULLS OVER (PARTITION BY csat, TO_DATE(closed_at) ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_requester_id,
        NTILE(5) OVER (PARTITION BY channel, agent_team ORDER BY csat NULLS LAST, id) AS csat_quintile,
        ROUND(requester_id - AVG(requester_id) OVER w_closed_at_month, 2) AS requester_id_vs_closed_at_month_avg,
        MAX(requester_id) OVER (PARTITION BY channel, agent_team ORDER BY csat NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS requester_id_max_2_prior,
        FIRST_VALUE(agent_hired_on) IGNORE NULLS OVER (PARTITION BY channel, agent_team ORDER BY csat NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_agent_hired_on
    FROM enriched_agent
    WINDOW w_closed_at_month AS (PARTITION BY DATE_TRUNC('MONTH', closed_at))
),
leaders AS (
    -- Top 3 per group
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        agent_hired_on,
        agent_team,
        agent_tier,
        distinct_channel_in_closed_at_month,
        closed_at_rank,
        minutes_since_prev_opened_at,
        closed_at_month_min_requester_id,
        second_closed_at,
        next2_csat,
        rows_in_closed_at_month,
        first_known_requester_id,
        csat_quintile,
        requester_id_vs_closed_at_month_avg,
        requester_id_max_2_prior,
        first_known_agent_hired_on
    FROM windowed
    WHERE requester_id_rank <= 3
),
windowed_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        opened_at,
        csat,
        agent_team,
        agent_tier,
        distinct_channel_in_closed_at_month,
        closed_at_rank,
        closed_at_month_min_requester_id,
        second_closed_at,
        next2_csat,
        first_known_requester_id,
        requester_id_vs_closed_at_month_avg,
        requester_id_max_2_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY distinct_channel_in_closed_at_month ORDER BY closed_at, id), 3) AS closed_at_percent_rank,
        SUM(first_known_requester_id) OVER (PARTITION BY distinct_channel_in_closed_at_month ORDER BY closed_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS first_known_requester_id_sum_3,
        ROUND(first_known_requester_id / NULLIF(SUM(first_known_requester_id) OVER (PARTITION BY distinct_channel_in_closed_at_month), 0), 4) AS first_known_requester_id_share_of_distinct_chann
    FROM leaders
)
SELECT
    id,
    csat,
    agent_team,
    agent_tier,
    closed_at_rank,
    closed_at_month_min_requester_id,
    next2_csat,
    requester_id_vs_closed_at_month_avg,
    requester_id_max_2_prior,
    closed_at_percent_rank,
    first_known_requester_id_share_of_distinct_chann,
    LOWER(REGEXP_REPLACE(TRIM(agent_team), ' ', '_')) AS agent_team_slug,
    ROUND(first_known_requester_id / NULLIF(closed_at_rank, 0), 3) AS first_known_requester_id_per_closed_at_rank
FROM windowed_2
