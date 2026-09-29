WITH valid_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at
    FROM tickets
    WHERE channel = 'phone'
      AND priority = 'P2'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(closed_at) OVER (ORDER BY closed_at NULLS LAST, id) AS prev_closed_at,
        ROUND(CUME_DIST() OVER w_agent_id_by_closed_at_ties, 3) AS closed_at_cume_dist,
        LAG(opened_at, 3) OVER w_all_by_closed_at AS prev3_opened_at,
        ROW_NUMBER() OVER w_agent_id_by_opened_at AS opened_at_seq,
        LAG(closed_at) OVER w_agent_id_channel_by_opened_a AS prev_closed_at_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(opened_at) ORDER BY opened_at), 3) AS opened_at_percent_rank,
        LEAD(opened_at) OVER (PARTITION BY agent_id ORDER BY closed_at, id) AS next_opened_at,
        NTH_VALUE(opened_at, 2) OVER (PARTITION BY MONTH(opened_at) ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER w_opened_at_month_by_opened_at_3)) / 60, 1) AS minutes_since_prev_closed_at,
        ROW_NUMBER() OVER (ORDER BY closed_at NULLS LAST, id) AS rn,
        RANK() OVER w_agent_id_channel_by_opened_a_2 AS opened_at_rank,
        NTILE(4) OVER (PARTITION BY agent_id ORDER BY closed_at, id) AS closed_at_quartile,
        FIRST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY agent_id ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_closed_at,
        LAST_VALUE(closed_at) OVER (PARTITION BY agent_id, channel ORDER BY opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_closed_at
    FROM valid_tickets
    WINDOW w_agent_id_by_closed_at_ties AS (PARTITION BY agent_id ORDER BY closed_at), w_all_by_closed_at AS (ORDER BY closed_at NULLS LAST, id), w_agent_id_by_opened_at AS (PARTITION BY agent_id ORDER BY opened_at ASC NULLS LAST, id), w_agent_id_channel_by_opened_a AS (PARTITION BY agent_id, channel ORDER BY opened_at ASC, id), w_opened_at_month_by_opened_at_3 AS (PARTITION BY MONTH(opened_at) ORDER BY opened_at, id), w_agent_id_channel_by_opened_a_2 AS (PARTITION BY agent_id, channel ORDER BY opened_at ASC)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM sequenced
    WHERE rn <= 5
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        rn - LAG(rn) OVER (PARTITION BY channel, priority ORDER BY rn DESC NULLS FIRST, id) AS rn_change,
        ROUND(opened_at_rank / NULLIF(SUM(opened_at_rank) OVER (PARTITION BY rn), 0), 4) AS opened_at_rank_share_of_rn,
        MAX(opened_at_rank) OVER (PARTITION BY priority, opened_at_seq ORDER BY prev_closed_at_2 ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS opened_at_rank_max_3
    FROM first_rows
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    closed_at,
    priority,
    channel,
    tags,
    minutes_opened_at_to_closed_at,
    prev_closed_at,
    closed_at_cume_dist,
    prev3_opened_at,
    opened_at_seq,
    prev_closed_at_2,
    opened_at_percent_rank,
    next_opened_at,
    minutes_since_prev_closed_at,
    rn,
    opened_at_rank,
    last_closed_at,
    rn_change,
    opened_at_rank_share_of_rn,
    opened_at_rank_max_3
FROM with_windows
ORDER BY opened_at_seq NULLS LAST, rn_change DESC, id
LIMIT 10
