WITH base_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags,
        SUBSTRING(TRIM(channel), 1, 3) AS channel_prefix,
        CASE WHEN csat >= 4 THEN 'A' WHEN csat >= 2 THEN 'B' ELSE 'C' END AS csat_tier
    FROM tickets
    WHERE csat >= 2
      AND opened_at < TIMESTAMP '2025-03-02 10:00:00'
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        opened_at,
        closed_at,
        priority,
        csat_tier,
        ROW_NUMBER() OVER w_agent_id_csat_by_csat AS rn,
        csat - LAG(csat) OVER (ORDER BY opened_at, id) AS csat_change,
        ROUND(requester_id - AVG(requester_id) OVER w_csat_agent_id, 2) AS requester_id_vs_csat_agent_id_avg,
        RANK() OVER (PARTITION BY MONTH(opened_at), csat_tier ORDER BY opened_at, id) AS opened_at_rank,
        MIN(requester_id) OVER (PARTITION BY priority ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_requester_id_so_far,
        ROUND(csat - AVG(csat) OVER w_csat_agent_id, 2) AS csat_vs_csat_agent_id_avg,
        ROUND(AVG(requester_id) OVER (ORDER BY opened_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_3,
        LAST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY agent_id, csat ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_closed_at,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY TO_DATE(closed_at) ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY csat_tier)) AS distinct_priority_in_csat_tier,
        NTH_VALUE(closed_at, 2) OVER (ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at_2,
        LAST_VALUE(opened_at) OVER (PARTITION BY TO_DATE(closed_at) ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_opened_at,
        SIZE(COLLECT_SET(csat_tier) OVER (PARTITION BY agent_id)) AS distinct_csat_tier_in_agent_id,
        LAST_VALUE(opened_at) OVER (PARTITION BY agent_id, csat ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_opened_at_2
    FROM base_tickets
    WINDOW w_agent_id_csat_by_csat AS (PARTITION BY agent_id, csat ORDER BY csat, id), w_csat_agent_id AS (PARTITION BY csat, agent_id)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        opened_at,
        closed_at,
        priority,
        csat_tier,
        csat_change,
        requester_id_vs_csat_agent_id_avg,
        opened_at_rank,
        min_requester_id_so_far,
        requester_id_avg_3,
        second_closed_at,
        distinct_priority_in_csat_tier,
        last_opened_at,
        last_opened_at_2,
        ROUND(AVG(opened_at_rank) OVER (PARTITION BY TO_DATE(last_known_closed_at), MONTH(last_opened_at)), 2) AS last_known_closed_at_to_date_last_opened_at_mont,
        MAX(opened_at_rank) OVER (ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_opened_at_rank_so_far,
        ROUND((UNIX_TIMESTAMP(last_known_closed_at) - UNIX_TIMESTAMP(LAG(last_known_closed_at) OVER w_all_by_opened_at)) / 60, 1) AS minutes_since_prev_last_known_closed_at,
        SUM(opened_at_rank) OVER (ORDER BY opened_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS opened_at_rank_sum_6_prior
    FROM with_trends
    WINDOW w_all_by_opened_at AS (ORDER BY opened_at, id)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(closed_at, 'yyyy-MM') AS closed_at_ym,
        SUM(distinct_priority_in_csat_tier) AS total_distinct_priority_in_csat_tier,
        MIN(closed_at) AS first_closed_at,
        ROUND(SUM(last_known_closed_at_to_date_last_opened_at_mont) / NULLIF(SUM(max_opened_at_rank_so_far), 0), 3) AS last_known_closed_at_to_date_last_opened_at_mont_2
    FROM with_windows
    GROUP BY DATE_FORMAT(closed_at, 'yyyy-MM')
),
with_windows_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(first_closed_at, 2) OVER (PARTITION BY YEAR(first_closed_at) ORDER BY last_known_closed_at_to_date_last_opened_at_mont_2 DESC, closed_at_ym ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_closed_at
    FROM rollup_base
)
-- Final projection
SELECT
    closed_at_ym,
    total_distinct_priority_in_csat_tier,
    last_known_closed_at_to_date_last_opened_at_mont_2,
    second_first_closed_at,
    DATEDIFF(DATE '2025-06-30', second_first_closed_at) AS days_since_second_first_closed_at,
    ROUND((UNIX_TIMESTAMP(first_closed_at) - UNIX_TIMESTAMP(second_first_closed_at)) / 3600, 1) AS hours_second_first_closed_at_to_first_closed_at
FROM with_windows_2
WHERE closed_at_ym IS NOT NULL
ORDER BY hours_second_first_closed_at_to_first_closed_at DESC
