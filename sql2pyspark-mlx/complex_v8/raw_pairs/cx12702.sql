WITH valid_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('YEAR', event_at) AS event_at_year
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-31 13:00:00'
      AND ticket_id IS NOT NULL
),
enriched AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_year,
        t.tags AS ticket_tags,
        t.closed_at AS ticket_closed_at
    FROM valid_events s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE MONTH(t.closed_at) IN (2, 3)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(event_at_year) IGNORE NULLS OVER (ORDER BY event_at_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_year,
        RANK() OVER w_all_by_event_at_year_ties AS event_at_year_rank,
        LEAD(event_at) OVER (PARTITION BY event_type ORDER BY event_at_year DESC NULLS FIRST, id) AS next_event_at,
        ROUND((UNIX_TIMESTAMP(event_at_year) - UNIX_TIMESTAMP(LAG(event_at_year) OVER w_all_by_event_at_year)) / 60, 1) AS minutes_since_prev_event_at_year,
        LAST_VALUE(ticket_closed_at) OVER (PARTITION BY event_type ORDER BY event_at_year DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ticket_closed_at,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type ORDER BY event_at_year DESC NULLS FIRST), 3) AS event_at_year_cume_dist,
        NTILE(10) OVER w_event_type_by_event_at_year AS event_at_year_decile,
        DENSE_RANK() OVER (PARTITION BY event_type ORDER BY event_at_year DESC NULLS FIRST) AS event_at_year_dense_rank,
        ROW_NUMBER() OVER w_event_type_by_event_at_year AS rn,
        ROUND(CUME_DIST() OVER w_ticket_closed_at_year_ticket, 3) AS ticket_closed_at_cume_dist,
        ROW_NUMBER() OVER (ORDER BY event_at_year DESC, id) AS seq_no,
        FIRST_VALUE(event_at_year) OVER (PARTITION BY YEAR(ticket_closed_at), ticket_id ORDER BY ticket_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at_year,
        LAG(ticket_closed_at, 3) OVER w_event_type_by_event_at_year AS prev3_ticket_closed_at,
        NTH_VALUE(ticket_closed_at, 2) OVER (ORDER BY event_at_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_closed_at
    FROM enriched
    WINDOW w_all_by_event_at_year_ties AS (ORDER BY event_at_year DESC), w_all_by_event_at_year AS (ORDER BY event_at_year DESC, id), w_event_type_by_event_at_year AS (PARTITION BY event_type ORDER BY event_at_year DESC NULLS FIRST, id), w_ticket_closed_at_year_ticket AS (PARTITION BY YEAR(ticket_closed_at), ticket_id ORDER BY ticket_closed_at)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        event_at_year,
        ticket_tags,
        last_known_event_at_year,
        next_event_at,
        last_ticket_closed_at,
        event_at_year_cume_dist,
        event_at_year_decile,
        ticket_closed_at_cume_dist,
        first_event_at_year,
        second_ticket_closed_at,
        COUNT(*) OVER w_actor_event_at_year_dense_ra AS rows_in_actor_event_at_year_dense_rank,
        DENSE_RANK() OVER (PARTITION BY YEAR(second_ticket_closed_at), event_at_year_rank ORDER BY first_event_at_year DESC) AS first_event_at_year_dense_rank,
        COUNT(*) OVER (PARTITION BY YEAR(second_ticket_closed_at), event_at_year_rank ORDER BY first_event_at_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(ticket_closed_at_cume_dist) OVER (PARTITION BY ticket_id, YEAR(second_ticket_closed_at) ORDER BY first_event_at_year DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS ticket_closed_at_cume_dist_avg_3_prior,
        NTH_VALUE(last_ticket_closed_at, 2) OVER (PARTITION BY YEAR(second_ticket_closed_at), event_at_year_rank ORDER BY first_event_at_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_ticket_closed_at,
        SUM(event_at_year_dense_rank) OVER (PARTITION BY ticket_id, YEAR(second_ticket_closed_at) ORDER BY first_event_at_year DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS event_at_year_dense_rank_sum_7,
        LEAD(last_known_event_at_year) OVER w_ticket_id_second_ticket_clos_3 AS next_last_known_event_at_year
    FROM sequenced
    WINDOW w_actor_event_at_year_dense_ra AS (PARTITION BY actor, event_at_year_dense_rank), w_ticket_id_second_ticket_clos_3 AS (PARTITION BY ticket_id, YEAR(second_ticket_closed_at) ORDER BY first_event_at_year DESC, id)
),
grouped AS (
    -- Group-level totals
    SELECT
        actor,
        DATE_FORMAT(second_last_ticket_closed_at, 'yyyy-MM') AS second_last_ticket_closed_at_ym,
        MIN(last_known_event_at_year) AS first_last_known_event_at_year,
        ROUND(AVG(event_at_year_decile), 2) AS avg_event_at_year_decile,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(ticket_closed_at_cume_dist_avg_3_prior, 0.5) AS median_ticket_closed_at_cume_dist_avg_3_prior
    FROM with_trends
    WHERE rows_in_actor_event_at_year_dense_rank < 16
    GROUP BY actor, DATE_FORMAT(second_last_ticket_closed_at, 'yyyy-MM')
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        cnt - LAG(cnt) OVER (PARTITION BY distinct_ids ORDER BY median_ticket_closed_at_cume_dist_avg_3_prior DESC, actor, second_last_ticket_closed_at_ym) AS cnt_change,
        COUNT(*) OVER (PARTITION BY actor) AS rows_in_actor,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_ids ORDER BY median_ticket_closed_at_cume_dist_avg_3_prior DESC), 3) AS median_ticket_closed_at_cume_dist_avg_3_prior_cu
    FROM grouped
)
-- Final projection
SELECT
    actor,
    second_last_ticket_closed_at_ym,
    first_last_known_event_at_year,
    avg_event_at_year_decile,
    cnt,
    distinct_ids,
    median_ticket_closed_at_cume_dist_avg_3_prior,
    median_ticket_closed_at_cume_dist_avg_3_prior_cu
FROM with_windows
WHERE rows_in_actor <= 14
ORDER BY cnt DESC NULLS FIRST
