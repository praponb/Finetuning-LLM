WITH valid_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        tags,
        ROUND(requester_id / NULLIF(csat, 0), 3) AS requester_id_per_csat,
        COALESCE(requester_id, 0) AS requester_id_filled
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'bug')
      AND channel = 'email'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY channel, agent_id ORDER BY requester_id_per_csat DESC, id) AS requester_id_per_csat_quintile,
        ROUND(AVG(requester_id_per_csat) OVER (PARTITION BY channel ORDER BY opened_at DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS requester_id_per_csat_avg_3_prior,
        NTH_VALUE(opened_at, 2) OVER (PARTITION BY channel, agent_id ORDER BY requester_id_per_csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY MONTH(opened_at), channel)) AS distinct_agent_id_in_opened_at_month_channel,
        LEAD(requester_id_filled) OVER (PARTITION BY YEAR(opened_at) ORDER BY requester_id_per_csat DESC, id) AS next_requester_id_filled,
        RANK() OVER w_channel_by_opened_at_ties AS opened_at_rank,
        ROUND(AVG(requester_id_filled) OVER (PARTITION BY channel ORDER BY opened_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS requester_id_filled_avg_2_prior,
        ROUND(PERCENT_RANK() OVER w_channel_agent_id_by_requeste_3, 3) AS requester_id_per_csat_percent_rank,
        ROUND(requester_id_filled / NULLIF(SUM(requester_id_filled) OVER (PARTITION BY MONTH(opened_at), channel), 0), 4) AS requester_id_filled_share_of_opened_at_month_cha,
        COUNT(*) OVER w_channel AS rows_in_channel,
        ROUND(PERCENT_RANK() OVER (PARTITION BY channel, agent_id ORDER BY requester_id_per_csat DESC, id), 3) AS requester_id_per_csat_percent_rank_2,
        LEAD(requester_id_per_csat) OVER (PARTITION BY channel, agent_id ORDER BY requester_id_filled DESC NULLS FIRST, id) AS next_requester_id_per_csat,
        LAG(requester_id_filled) OVER w_channel_by_opened_at AS prev_requester_id_filled,
        ROUND(CUME_DIST() OVER w_channel_agent_id_by_requeste, 3) AS requester_id_per_csat_cume_dist,
        ROUND(requester_id_per_csat - AVG(requester_id_per_csat) OVER (PARTITION BY channel), 2) AS requester_id_per_csat_vs_channel_avg
    FROM valid_tickets
    WINDOW w_channel_by_opened_at_ties AS (PARTITION BY channel ORDER BY opened_at DESC), w_channel_agent_id_by_requeste_3 AS (PARTITION BY channel, agent_id ORDER BY requester_id_per_csat DESC), w_channel AS (PARTITION BY channel), w_channel_by_opened_at AS (PARTITION BY channel ORDER BY opened_at DESC, id), w_channel_agent_id_by_requeste AS (PARTITION BY channel, agent_id ORDER BY requester_id_per_csat DESC, id)
)
SELECT
    id,
    agent_id,
    opened_at,
    channel,
    requester_id_filled,
    requester_id_per_csat_quintile,
    distinct_agent_id_in_opened_at_month_channel,
    next_requester_id_filled,
    opened_at_rank,
    requester_id_filled_avg_2_prior,
    requester_id_per_csat_percent_rank,
    rows_in_channel,
    requester_id_per_csat_percent_rank_2,
    next_requester_id_per_csat,
    prev_requester_id_filled,
    LOWER(TRIM(channel)) AS channel_clean
FROM metrics
ORDER BY rows_in_channel DESC
