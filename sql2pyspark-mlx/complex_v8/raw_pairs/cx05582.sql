WITH base_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM ticket_events
    WHERE MONTH(event_at) IN (1, 2)
),
stats AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        event_type,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        ROUND(100 * COUNT(CASE WHEN event_type = 'reopened' THEN 1 END) / COUNT(*), 1) AS pct_event_type_reopened,
        COUNT(*) FILTER (WHERE event_type = 'reopened') AS n_event_type_reopened,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(CASE WHEN event_type <> 'resolved' THEN 1 ELSE 0 END) AS n_event_type_resolved,
        COUNT(*) FILTER (WHERE actor IN ('system', 'customer')) AS n_actor_in_system_customer
    FROM base_events
    GROUP BY DATE_TRUNC('MONTH', event_at), event_type
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM stats
    WHERE distinct_ids > (SELECT MIN(distinct_ids) FROM stats)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY event_type ORDER BY n_actor_in_system_customer) AS n_actor_in_system_customer_dense_rank,
        NTH_VALUE(distinct_ids, 2) OVER (PARTITION BY event_type ORDER BY n_actor_in_system_customer, event_at_month, event_type ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_ids,
        ROUND(distinct_ids - AVG(distinct_ids) OVER w_n_actor_in_system_customer, 2) AS distinct_ids_vs_n_actor_in_system_customer_avg,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY n_actor_in_system_customer, event_at_month, event_type) AS seq_no,
        ROUND(n_event_type_resolved - AVG(n_event_type_resolved) OVER (PARTITION BY n_actor_in_system_customer), 2) AS n_event_type_resolved_vs_n_actor_in_system_custo
    FROM without_matches
    WINDOW w_n_actor_in_system_customer AS (PARTITION BY n_actor_in_system_customer)
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM windowed
    WHERE seq_no < (SELECT MAX(seq_no) FROM windowed)
)
-- Final projection
SELECT
    event_at_month,
    event_type,
    actor_list,
    pct_event_type_reopened,
    n_event_type_reopened,
    distinct_ids,
    n_actor_in_system_customer,
    n_actor_in_system_customer_dense_rank,
    second_distinct_ids,
    distinct_ids_vs_n_actor_in_system_customer_avg,
    seq_no,
    n_event_type_resolved_vs_n_actor_in_system_custo,
    DATE_TRUNC('MONTH', event_at_month) AS event_at_month_month,
    DATE_FORMAT(event_at_month, 'yyyy-MM-dd HH:00') AS event_at_month_label
FROM filtered
ORDER BY event_at_month_month DESC NULLS FIRST, event_at_month DESC
