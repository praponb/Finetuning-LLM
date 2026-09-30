WITH scoped AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        tier,
        CASE WHEN tier IN ('T1', 'T3') THEN 'group_a' WHEN tier = 'T2' THEN 'group_b' ELSE 'other' END AS tier_group,
        CASE WHEN tier IN ('T2', 'T3') THEN 'group_a' WHEN tier = 'T1' THEN 'group_b' ELSE 'other' END AS tier_group_2
    FROM support_agents
    WHERE hired_on BETWEEN DATE '2024-07-09' AND DATE '2025-11-09'
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(*) AS ticket_count,
        MAX(csat) AS max_ticket_csat,
        MIN(closed_at) AS first_ticket_closed_at
    FROM tickets
    GROUP BY agent_id
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        t.max_ticket_csat,
        t.first_ticket_closed_at
    FROM scoped s
    LEFT JOIN ticket_stats t ON t.agent_id = s.id
),
detail_rows_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.agent_name,
        s.team,
        s.tier_group,
        s.tier_group_2,
        s.max_ticket_csat,
        s.first_ticket_closed_at,
        t.id AS ticket_id,
        t.channel AS ticket_channel,
        t.requester_id AS ticket_requester_id
    FROM profiled s
    LEFT JOIN tickets t ON t.agent_id = s.id AND t.requester_id BETWEEN 2 AND 22
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        max_ticket_csat,
        COUNT(DISTINCT tier_group) AS distinct_tier_groups,
        COUNT(*) AS cnt
    FROM detail_rows_tickets
    WHERE max_ticket_csat > 3
    GROUP BY max_ticket_csat
),
derived AS (
    -- Derive helper columns
    SELECT
        max_ticket_csat,
        distinct_tier_groups,
        LEAST(cnt, distinct_tier_groups) AS least_cnt_distinct_tier_groups
    FROM rollup_base
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM derived
    WHERE distinct_tier_groups > (SELECT AVG(distinct_tier_groups) FROM derived)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER w_max_ticket_csat_by_least_cnt AS least_cnt_distinct_tier_groups_quintile,
        MAX(max_ticket_csat) OVER (PARTITION BY max_ticket_csat ORDER BY least_cnt_distinct_tier_groups DESC, max_ticket_csat ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS max_ticket_csat_max_6_prior,
        LAST_VALUE(max_ticket_csat) IGNORE NULLS OVER (PARTITION BY max_ticket_csat ORDER BY least_cnt_distinct_tier_groups DESC, max_ticket_csat ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_max_ticket_csat,
        ROW_NUMBER() OVER (PARTITION BY max_ticket_csat ORDER BY least_cnt_distinct_tier_groups DESC, max_ticket_csat) AS least_cnt_distinct_tier_groups_seq,
        LAST_VALUE(max_ticket_csat) OVER (PARTITION BY max_ticket_csat ORDER BY least_cnt_distinct_tier_groups DESC, max_ticket_csat ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_max_ticket_csat,
        NTH_VALUE(max_ticket_csat, 2) OVER (PARTITION BY max_ticket_csat ORDER BY least_cnt_distinct_tier_groups DESC, max_ticket_csat ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_max_ticket_csat
    FROM without_matches
    WINDOW w_max_ticket_csat_by_least_cnt AS (PARTITION BY max_ticket_csat ORDER BY least_cnt_distinct_tier_groups DESC, max_ticket_csat)
)
-- Final projection
SELECT
    max_ticket_csat,
    least_cnt_distinct_tier_groups_quintile,
    max_ticket_csat_max_6_prior,
    last_known_max_ticket_csat,
    least_cnt_distinct_tier_groups_seq,
    last_max_ticket_csat,
    LEAST(distinct_tier_groups, last_max_ticket_csat) AS least_distinct_tier_groups_last_max_ticket_csat,
    ROUND(second_max_ticket_csat / NULLIF(distinct_tier_groups, 0), 3) AS second_max_ticket_csat_per_distinct_tier_groups
FROM metrics
WHERE max_ticket_csat >= 3
ORDER BY max_ticket_csat DESC
