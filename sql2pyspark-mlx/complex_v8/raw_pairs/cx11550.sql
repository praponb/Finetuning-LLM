WITH base AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on
    FROM support_agents
    WHERE team = 'Onboarding'
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, MIN(closed_at) AS first_ticket_closed_at
    FROM tickets
    GROUP BY agent_id
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.first_ticket_closed_at
    FROM base s
    LEFT JOIN ticket_stats t ON t.agent_id = s.id
),
totals AS (
    -- Group-level totals
    SELECT
        tier,
        MIN(first_ticket_closed_at) AS first_first_ticket_closed_at,
        DATEDIFF(MAX(hired_on), MIN(hired_on)) AS hired_on_span_days,
        COUNT(DISTINCT tier) AS distinct_tiers,
        ARRAY_SORT(COLLECT_SET(agent_name)) AS agent_name_list,
        SUM(CASE WHEN tier IS NOT NULL THEN 1 ELSE 0 END) AS n_tier_is_not_null
    FROM with_counts
    GROUP BY 1
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(hired_on_span_days) OVER (ORDER BY first_first_ticket_closed_at ASC, tier) AS prev_hired_on_span_days,
        NTILE(5) OVER (ORDER BY first_first_ticket_closed_at ASC, tier) AS first_first_ticket_closed_at_quintile,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', first_first_ticket_closed_at) ORDER BY first_first_ticket_closed_at DESC, tier), 3) AS first_first_ticket_closed_at_cume_dist
    FROM totals
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        tier,
        first_first_ticket_closed_at,
        hired_on_span_days,
        distinct_tiers,
        agent_name_list,
        n_tier_is_not_null,
        first_first_ticket_closed_at_quintile,
        first_first_ticket_closed_at_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY first_first_ticket_closed_at_quintile, tier ORDER BY first_first_ticket_closed_at, tier) AS first_first_ticket_closed_at_seq,
        DENSE_RANK() OVER w_first_first_ticket_closed_at_2 AS first_first_ticket_closed_at_dense_rank,
        MAX(distinct_tiers) OVER (PARTITION BY YEAR(first_first_ticket_closed_at), n_tier_is_not_null ORDER BY prev_hired_on_span_days, tier ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS distinct_tiers_max_1_prior,
        LAST_VALUE(n_tier_is_not_null) IGNORE NULLS OVER (PARTITION BY tier, first_first_ticket_closed_at_quintile ORDER BY hired_on_span_days DESC, tier ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_n_tier_is_not_null,
        ROUND(distinct_tiers - AVG(distinct_tiers) OVER (PARTITION BY first_first_ticket_closed_at_quintile), 2) AS distinct_tiers_vs_first_first_ticket_closed_at_q
    FROM windowed
    WINDOW w_first_first_ticket_closed_at_2 AS (PARTITION BY first_first_ticket_closed_at_quintile, tier ORDER BY first_first_ticket_closed_at, tier)
)
SELECT
    first_first_ticket_closed_at_dense_rank,
    first_first_ticket_closed_at_seq,
    distinct_tiers,
    GROUPING(first_first_ticket_closed_at_dense_rank) AS is_total_first_first_ticket_closed_at_dense_rank,
    ROUND(SUM(distinct_tiers) / NULLIF(SUM(first_first_ticket_closed_at_cume_dist), 0), 3) AS distinct_tiers_to_first_first_ticket_closed_at_c,
    MIN(hired_on_span_days) AS min_hired_on_span_days,
    SUM(distinct_tiers) AS total_distinct_tiers
FROM sequenced
GROUP BY GROUPING SETS ((first_first_ticket_closed_at_dense_rank, first_first_ticket_closed_at_seq, distinct_tiers), (first_first_ticket_closed_at_dense_rank), (first_first_ticket_closed_at_seq), ())
ORDER BY first_first_ticket_closed_at_dense_rank NULLS LAST, first_first_ticket_closed_at_seq NULLS LAST, distinct_tiers NULLS LAST
