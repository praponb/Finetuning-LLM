WITH recent_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('YEAR', event_at) AS event_at_year
    FROM ticket_events
    WHERE event_type <> 'created'
      AND MONTH(event_at) IN (1, 2)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_year,
        NTILE(3) OVER (ORDER BY event_at_year DESC, id) AS event_at_year_tercile,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor, ticket_id ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROW_NUMBER() OVER (PARTITION BY actor, ticket_id ORDER BY event_at ASC, id) AS seq_no,
        NTILE(5) OVER (ORDER BY event_at DESC NULLS FIRST, id) AS event_at_quintile,
        ROUND(CUME_DIST() OVER w_actor_ticket_id_by_event_at_3, 3) AS event_at_cume_dist,
        ROUND(CUME_DIST() OVER (ORDER BY event_at_year DESC), 3) AS event_at_year_cume_dist,
        NTILE(10) OVER (ORDER BY event_at DESC NULLS FIRST, id) AS event_at_decile,
        LAG(event_at_year, 2) OVER (PARTITION BY actor, ticket_id ORDER BY event_at ASC, id) AS prev2_event_at_year,
        RANK() OVER w_all_by_event_at_ties AS event_at_rank,
        LEAD(event_at_year) OVER (ORDER BY event_at_year DESC, id) AS next_event_at_year
    FROM recent_events
    WINDOW w_actor_ticket_id_by_event_at_3 AS (PARTITION BY actor, ticket_id ORDER BY event_at ASC), w_all_by_event_at_ties AS (ORDER BY event_at DESC NULLS FIRST)
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(event_at_year_tercile / 2) * 2 AS event_at_year_tercile_bucket,
        DATE_FORMAT(event_at_year, 'yyyy-MM') AS event_at_year_label,
        event_at_rank + event_at_quintile AS event_at_rank_plus_event_at_quintile
    FROM with_trends
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', event_at_year) AS event_at_year_month,
        ticket_id,
        COUNT(*) AS cnt,
        COUNT(*) FILTER (WHERE actor IN ('agent', 'customer')) AS n_actor_in_agent_customer,
        MAX(prev2_event_at_year) AS last_prev2_event_at_year,
        MAX(seq_no) AS max_seq_no,
        MAX(next_event_at_year) AS last_next_event_at_year
    FROM derived
    WHERE TO_DATE(event_at_year) < DATE '2025-02-17'
    GROUP BY DATE_TRUNC('MONTH', event_at_year), ticket_id
    HAVING COUNT(*) >= 2
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        event_at_year_month,
        ticket_id,
        n_actor_in_agent_customer,
        last_prev2_event_at_year,
        max_seq_no,
        last_next_event_at_year,
        LAST_VALUE(event_at_year_month) OVER (PARTITION BY MONTH(last_prev2_event_at_year) ORDER BY last_prev2_event_at_year ASC, event_at_year_month, ticket_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_year_month,
        RANK() OVER w_last_prev2_event_at_year_mon_2 AS last_prev2_event_at_year_rank,
        SIZE(COLLECT_SET(ticket_id) OVER w_cnt) AS distinct_ticket_id_in_cnt
    FROM per_group
    WINDOW w_last_prev2_event_at_year_mon_2 AS (PARTITION BY MONTH(last_prev2_event_at_year) ORDER BY last_prev2_event_at_year ASC), w_cnt AS (PARTITION BY cnt)
)
-- Final projection
SELECT
    event_at_year_month,
    ticket_id,
    n_actor_in_agent_customer,
    last_prev2_event_at_year,
    max_seq_no,
    last_next_event_at_year,
    last_event_at_year_month
FROM sequenced
ORDER BY max_seq_no DESC NULLS FIRST, last_prev2_event_at_year
