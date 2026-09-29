WITH cleaned_events AS (
    -- Scope the ticket_events rows
    SELECT *
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_all_by_event_at)) / 60, 1) AS minutes_since_prev_event_at,
        RANK() OVER (PARTITION BY YEAR(event_at), actor ORDER BY event_at) AS event_at_rank,
        ROUND(CUME_DIST() OVER (ORDER BY event_at NULLS LAST), 3) AS event_at_cume_dist,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type, actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        LAG(event_at, 2) OVER (PARTITION BY actor ORDER BY event_at DESC, id) AS prev2_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(event_at), actor ORDER BY event_at, id), 3) AS event_at_percent_rank,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND(CUME_DIST() OVER (ORDER BY event_at NULLS LAST, id), 3) AS event_at_cume_dist_2,
        LAG(event_at, 2) OVER (ORDER BY event_at NULLS LAST, id) AS prev2_event_at_2
    FROM cleaned_events
    WINDOW w_all_by_event_at AS (ORDER BY event_at DESC, id)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE event_at_rank = 1
      AND MONTH(event_at) IN (1, 3)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        actor,
        minutes_since_prev_event_at,
        event_at_rank,
        event_at_cume_dist,
        first_known_event_at,
        prev2_event_at,
        second_event_at,
        prev2_event_at_2,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY YEAR(first_known_event_at), actor ORDER BY event_at_cume_dist_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2,
        ROUND(AVG(minutes_since_prev_event_at) OVER (PARTITION BY YEAR(first_known_event_at), actor ORDER BY event_at_cume_dist_2 DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS minutes_since_prev_event_at_avg_7,
        ROUND(AVG(event_at_cume_dist) OVER (PARTITION BY TO_DATE(prev2_event_at), DATE_TRUNC('MONTH', second_event_at) ORDER BY event_at_cume_dist, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS event_at_cume_dist_avg_1_prior,
        FIRST_VALUE(prev2_event_at) OVER (PARTITION BY TO_DATE(prev2_event_at), DATE_TRUNC('MONTH', second_event_at) ORDER BY event_at_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_prev2_event_at,
        COUNT(*) OVER (PARTITION BY event_at_rank, YEAR(second_event_at)) AS rows_in_event_at_rank_second_event_at_year
    FROM leaders
),
per_group AS (
    -- Summarise per group
    SELECT
        YEAR(prev2_event_at_2) AS prev2_event_at_2_year,
        SUM(CASE WHEN actor = 'customer' THEN 1 ELSE 0 END) AS n_actor_customer,
        COUNT(*) AS n_rows
    FROM ranked
    GROUP BY 1
    HAVING COUNT(*) >= 2
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY n_actor_customer, prev2_event_at_2_year ORDER BY n_actor_customer DESC, prev2_event_at_2_year), 3) AS n_actor_customer_cume_dist,
        MAX(n_actor_customer) OVER w_n_rows AS n_rows_max_n_actor_customer
    FROM per_group
    WINDOW w_n_rows AS (PARTITION BY n_rows)
)
SELECT
    prev2_event_at_2_year,
    n_actor_customer_cume_dist,
    n_rows_max_n_actor_customer,
    ROUND(n_actor_customer / NULLIF(prev2_event_at_2_year, 0), 3) AS n_actor_customer_per_prev2_event_at_2_year,
    ROUND(n_actor_customer / NULLIF(prev2_event_at_2_year, 0), 3) AS n_actor_customer_per_prev2_event_at_2_year_2
FROM with_windows
ORDER BY n_rows_max_n_actor_customer DESC NULLS FIRST, prev2_event_at_2_year NULLS LAST
