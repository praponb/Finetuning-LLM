WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label,
        event_at + INTERVAL 1 DAYS AS event_at_plus_1d
    FROM ticket_events
    WHERE ticket_id >= 9
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_type,
        event_at_label,
        event_at_plus_1d,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(event_at_plus_1d) ORDER BY event_at DESC), 3) AS event_at_cume_dist,
        LEAD(event_at_plus_1d) OVER (PARTITION BY TO_DATE(event_at_plus_1d) ORDER BY event_at DESC, id) AS next_event_at_plus_1d,
        ROUND((UNIX_TIMESTAMP(event_at_plus_1d) - UNIX_TIMESTAMP(LAG(event_at_plus_1d) OVER (PARTITION BY event_type ORDER BY event_at_plus_1d DESC, id))) / 60, 1) AS minutes_since_prev_event_at_plus_1d,
        NTILE(3) OVER (PARTITION BY event_type ORDER BY event_at_plus_1d DESC, id) AS event_at_plus_1d_tercile,
        FIRST_VALUE(event_at_plus_1d) IGNORE NULLS OVER (PARTITION BY TO_DATE(event_at_plus_1d) ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_plus_1d,
        NTILE(10) OVER w_event_type_by_event_at_plus AS event_at_plus_1d_decile,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        LAG(event_at) OVER w_event_type_by_event_at AS prev_event_at,
        ROW_NUMBER() OVER w_event_at_plus_1d_year_ticket AS event_at_seq,
        DENSE_RANK() OVER w_event_at_plus_1d_to_date_by_2 AS event_at_dense_rank,
        ROW_NUMBER() OVER (ORDER BY event_at_plus_1d, id) AS event_at_plus_1d_seq
    FROM filtered
    WINDOW w_event_type_by_event_at_plus AS (PARTITION BY event_type ORDER BY event_at_plus_1d DESC, id), w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at, id), w_event_at_plus_1d_year_ticket AS (PARTITION BY YEAR(event_at_plus_1d), ticket_id ORDER BY event_at ASC, id), w_event_at_plus_1d_to_date_by_2 AS (PARTITION BY TO_DATE(event_at_plus_1d) ORDER BY event_at DESC, id)
),
sequenced_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(event_at_dense_rank) OVER (ORDER BY event_at_plus_1d_tercile DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS event_at_dense_rank_avg_3,
        LAST_VALUE(prev_event_at) IGNORE NULLS OVER (PARTITION BY event_at_seq, TO_DATE(first_known_event_at_plus_1d) ORDER BY event_at_plus_1d_tercile ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_event_at,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY event_at_plus_1d_seq)) AS distinct_event_type_in_event_at_plus_1d_seq,
        NTILE(5) OVER (ORDER BY event_at_plus_1d_tercile DESC, id) AS event_at_plus_1d_tercile_quintile
    FROM sequenced
),
per_group AS (
    -- Group-level totals
    SELECT
        event_at_plus_1d_seq,
        MAX(first_known_event_at_plus_1d) AS last_first_known_event_at_plus_1d,
        COUNT(DISTINCT event_type) AS distinct_event_types,
        MAX(event_at_plus_1d_decile) AS max_event_at_plus_1d_decile,
        ROUND(SUM(minutes_since_prev_event_at_plus_1d), 2) AS total_minutes_since_prev_event_at_plus_1d
    FROM sequenced_2
    GROUP BY event_at_plus_1d_seq
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(3) OVER w_max_event_at_plus_1d_decile AS distinct_event_types_tercile,
        ROUND(AVG(event_at_plus_1d_seq) OVER (PARTITION BY max_event_at_plus_1d_decile, event_at_plus_1d_seq ORDER BY distinct_event_types, event_at_plus_1d_seq ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS event_at_plus_1d_seq_avg_7,
        ROUND(AVG(max_event_at_plus_1d_decile) OVER (PARTITION BY max_event_at_plus_1d_decile, event_at_plus_1d_seq ORDER BY distinct_event_types, event_at_plus_1d_seq ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS max_event_at_plus_1d_decile_avg_3_prior,
        LAST_VALUE(distinct_event_types) IGNORE NULLS OVER (PARTITION BY max_event_at_plus_1d_decile, event_at_plus_1d_seq ORDER BY distinct_event_types, event_at_plus_1d_seq ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_distinct_event_types
    FROM per_group
    WINDOW w_max_event_at_plus_1d_decile AS (PARTITION BY max_event_at_plus_1d_decile, event_at_plus_1d_seq ORDER BY distinct_event_types, event_at_plus_1d_seq)
)
SELECT
    event_at_plus_1d_seq,
    distinct_event_types,
    max_event_at_plus_1d_decile,
    total_minutes_since_prev_event_at_plus_1d,
    distinct_event_types_tercile,
    max_event_at_plus_1d_decile_avg_3_prior,
    last_known_distinct_event_types,
    event_at_plus_1d_seq - distinct_event_types AS event_at_plus_1d_seq_minus_distinct_event_types,
    distinct_event_types + event_at_plus_1d_seq AS distinct_event_types_plus_event_at_plus_1d_seq
FROM with_trends
WHERE max_event_at_plus_1d_decile_avg_3_prior > 2.4
ORDER BY distinct_event_types_tercile
