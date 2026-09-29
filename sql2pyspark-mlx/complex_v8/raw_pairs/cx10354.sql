WITH recent AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        DATE_FORMAT(hired_on, 'yyyy-MM') AS hired_on_label
    FROM support_agents
    WHERE hired_on < DATE '2025-02-27'
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT SUM(t.requester_id) FROM tickets t WHERE t.agent_id = s.id) AS total_ticket_requester_id,
        (SELECT COUNT(*) FILTER (WHERE t.priority = 'P3') FROM tickets t WHERE t.agent_id = s.id) AS ticket_priority_count
    FROM recent s
),
stats AS (
    -- Group-level totals
    SELECT
        tier,
        DATE_FORMAT(hired_on, 'yyyy-MM') AS hired_on_ym,
        SUM(CASE WHEN tier IS NOT NULL THEN 1 ELSE 0 END) AS n_tier_is_not_null,
        ROUND(STDDEV(total_ticket_requester_id), 2) AS stddev_total_ticket_requester_id
    FROM summarized_rows
    WHERE LOWER(agent_name) LIKE '%au%'
    GROUP BY tier, DATE_FORMAT(hired_on, 'yyyy-MM')
    HAVING SUM(ticket_priority_count) > 2
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        tier,
        hired_on_ym,
        n_tier_is_not_null,
        stddev_total_ticket_requester_id,
        FIRST_VALUE(n_tier_is_not_null) OVER (PARTITION BY tier, n_tier_is_not_null ORDER BY n_tier_is_not_null, tier, hired_on_ym ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_n_tier_is_not_null,
        n_tier_is_not_null - LAG(n_tier_is_not_null) OVER w_n_tier_is_not_null_tier_by_n AS n_tier_is_not_null_change,
        ROUND(PERCENT_RANK() OVER w_tier_n_tier_is_not_null_by_n_2, 3) AS n_tier_is_not_null_percent_rank,
        SIZE(COLLECT_SET(tier) OVER w_n_tier_is_not_null) AS distinct_tier_in_n_tier_is_not_null,
        ROUND(CUME_DIST() OVER (PARTITION BY tier, n_tier_is_not_null ORDER BY n_tier_is_not_null), 3) AS n_tier_is_not_null_cume_dist,
        NTILE(4) OVER (PARTITION BY tier, n_tier_is_not_null ORDER BY n_tier_is_not_null, tier, hired_on_ym) AS n_tier_is_not_null_quartile
    FROM stats
    WINDOW w_n_tier_is_not_null_tier_by_n AS (PARTITION BY n_tier_is_not_null, tier ORDER BY n_tier_is_not_null DESC, tier, hired_on_ym), w_tier_n_tier_is_not_null_by_n_2 AS (PARTITION BY tier, n_tier_is_not_null ORDER BY n_tier_is_not_null), w_n_tier_is_not_null AS (PARTITION BY n_tier_is_not_null)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(n_tier_is_not_null - AVG(n_tier_is_not_null) OVER w_n_tier_is_not_null_tier, 2) AS n_tier_is_not_null_vs_n_tier_is_not_null_tier_av,
        ROUND(AVG(first_n_tier_is_not_null) OVER w_n_tier_is_not_null_tier, 2) AS n_tier_is_not_null_tier_avg_first_n_tier_is_not_,
        ROUND(AVG(first_n_tier_is_not_null) OVER (PARTITION BY n_tier_is_not_null, distinct_tier_in_n_tier_is_not_null ORDER BY distinct_tier_in_n_tier_is_not_null, tier, hired_on_ym ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS first_n_tier_is_not_null_avg_3_prior,
        n_tier_is_not_null - LAG(n_tier_is_not_null) OVER (PARTITION BY n_tier_is_not_null, distinct_tier_in_n_tier_is_not_null ORDER BY distinct_tier_in_n_tier_is_not_null, tier, hired_on_ym) AS n_tier_is_not_null_change_2,
        COUNT(*) OVER w_n_tier_is_not_null_tier AS rows_in_n_tier_is_not_null_tier
    FROM sequenced
    WINDOW w_n_tier_is_not_null_tier AS (PARTITION BY n_tier_is_not_null, tier)
)
SELECT
    tier,
    n_tier_is_not_null,
    stddev_total_ticket_requester_id,
    first_n_tier_is_not_null,
    n_tier_is_not_null_change,
    n_tier_is_not_null_percent_rank,
    n_tier_is_not_null_cume_dist,
    n_tier_is_not_null_quartile,
    n_tier_is_not_null_vs_n_tier_is_not_null_tier_av,
    first_n_tier_is_not_null_avg_3_prior
FROM with_windows
WHERE tier <> 'T2'
