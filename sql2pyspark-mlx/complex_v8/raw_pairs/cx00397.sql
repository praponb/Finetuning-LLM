WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        UPPER(TRIM(event_type)) AS event_type_clean,
        LOWER(TRIM(event_type)) AS event_type_clean_2
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-24'
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(TRIM(event_type_clean)) AS event_type_clean_clean,
        SUBSTRING(TRIM(event_type_clean_2), 1, 3) AS event_type_clean_2_prefix
    FROM prepared
),
filtered AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM tagged s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
            WHERE TO_DATE(t.event_at) < DATE '2025-02-06'
        )
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS('-', event_type_clean_clean, event_type) AS event_type_clean_clean_event_type_label,
        DATE_TRUNC('QUARTER', event_at) AS event_at_quarter
    FROM filtered
),
summary AS (
    -- Summarise per group
    SELECT
        ticket_id,
        DATE_FORMAT(event_at_quarter, 'yyyy-MM') AS event_at_quarter_ym,
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        COUNT(*) FILTER (WHERE actor IS NOT NULL) AS n_actor_is_not_null
    FROM labeled
    WHERE event_type <> 'created'
    GROUP BY 1, 2, 3
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY n_actor_is_not_null, ticket_id ORDER BY n_actor_is_not_null NULLS LAST, ticket_id, event_at_quarter_ym, event_at_month) AS n_actor_is_not_null_dense_rank,
        SUM(n_actor_is_not_null) OVER (PARTITION BY n_actor_is_not_null, ticket_id ORDER BY n_actor_is_not_null NULLS LAST, ticket_id, event_at_quarter_ym, event_at_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS n_actor_is_not_null_sum_3,
        NTILE(4) OVER w_ticket_id_n_actor_is_not_nul AS n_actor_is_not_null_quartile,
        ROUND(n_actor_is_not_null - AVG(n_actor_is_not_null) OVER (PARTITION BY n_actor_is_not_null), 2) AS n_actor_is_not_null_vs_n_actor_is_not_null_avg,
        DENSE_RANK() OVER (PARTITION BY ticket_id, n_actor_is_not_null ORDER BY n_actor_is_not_null ASC) AS n_actor_is_not_null_dense_rank_2,
        LAST_VALUE(event_at_month) IGNORE NULLS OVER (PARTITION BY ticket_id, n_actor_is_not_null ORDER BY n_actor_is_not_null ASC, ticket_id, event_at_quarter_ym, event_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_month
    FROM summary
    WINDOW w_ticket_id_n_actor_is_not_nul AS (PARTITION BY ticket_id, n_actor_is_not_null ORDER BY n_actor_is_not_null ASC, ticket_id, event_at_quarter_ym, event_at_month)
)
SELECT
    ticket_id,
    event_at_quarter_ym,
    event_at_month,
    event_type_list,
    n_actor_is_not_null_dense_rank,
    n_actor_is_not_null_sum_3,
    n_actor_is_not_null_vs_n_actor_is_not_null_avg,
    n_actor_is_not_null_dense_rank_2
FROM metrics
WHERE n_actor_is_not_null_sum_3 >= 4
ORDER BY n_actor_is_not_null_sum_3 DESC NULLS FIRST
