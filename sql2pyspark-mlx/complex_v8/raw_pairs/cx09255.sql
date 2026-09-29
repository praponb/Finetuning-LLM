WITH filtered_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label_2,
        CONCAT_WS('|', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE event_type = 'escalated'
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        event_type_actor_label,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label_3
    FROM filtered_events
    WHERE event_at < TIMESTAMP '2025-02-12 12:00:00'
),
stats AS (
    -- Group-level totals
    SELECT
        actor_or_unknown,
        actor,
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        COUNT(*) AS row_count,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        ROUND(100 * COUNT(CASE WHEN actor IN ('system', 'customer') THEN 1 END) / COUNT(*), 1) AS pct_actor_in_system_customer
    FROM tagged
    GROUP BY actor_or_unknown, actor, DATE_TRUNC('MONTH', event_at)
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE row_count > (SELECT AVG(row_count) FROM stats)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(row_count) OVER (PARTITION BY actor, row_count) AS actor_row_count_max_row_count,
        SIZE(COLLECT_SET(actor_or_unknown) OVER (PARTITION BY actor, row_count)) AS distinct_actor_or_unknown_in_actor_row_count,
        SUM(pct_actor_in_system_customer) OVER (PARTITION BY actor, row_count) AS actor_row_count_sum_pct_actor_in_system_customer
    FROM filtered
),
labeled AS (
    -- Derive helper columns
    SELECT
        actor_or_unknown,
        actor,
        event_at_month,
        row_count,
        actor_row_count_max_row_count,
        distinct_actor_or_unknown_in_actor_row_count,
        actor_row_count_sum_pct_actor_in_system_customer,
        SIZE(actor_list) AS actor_list_count,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown_2
    FROM windowed
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(actor_list_count, 2) OVER w_actor_or_unknown_actor_by_ac AS prev2_actor_list_count,
        ROW_NUMBER() OVER (PARTITION BY distinct_actor_or_unknown_in_actor_row_count, actor_or_unknown ORDER BY row_count ASC, actor_or_unknown, actor, event_at_month) AS row_count_seq
    FROM labeled
    WINDOW w_actor_or_unknown_actor_by_ac AS (PARTITION BY actor_or_unknown, actor ORDER BY actor_row_count_max_row_count DESC, actor_or_unknown, actor, event_at_month)
)
-- Final projection
SELECT
    actor_or_unknown,
    actor,
    event_at_month,
    distinct_actor_or_unknown_in_actor_row_count,
    actor_row_count_sum_pct_actor_in_system_customer,
    actor_or_unknown_2,
    prev2_actor_list_count,
    row_count_seq,
    actor_list_count + prev2_actor_list_count AS actor_list_count_plus_prev2_actor_list_count,
    DATE_FORMAT(event_at_month, 'yyyy-MM-dd HH:00') AS event_at_month_label
FROM metrics
