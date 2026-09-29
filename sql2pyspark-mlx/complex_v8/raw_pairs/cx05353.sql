WITH valid_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id <= 40
),
deduped AS (
    -- Keep the latest row per ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id DESC) AS row_num
        FROM valid_events
    ) ranked
    WHERE row_num = 1
),
unique_rows AS (
    -- Keep the latest row per actor
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor ORDER BY event_at DESC, id DESC) AS row_num
        FROM deduped
    ) ranked
    WHERE row_num = 1
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        ticket_id,
        actor,
        event_type,
        COUNT(DISTINCT ticket_id) AS distinct_ticket_ids,
        COUNT(*) AS row_count,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(event_at) AS last_event_at
    FROM unique_rows
    GROUP BY ticket_id, actor, event_type
    HAVING COUNT(*) >= 2
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(distinct_ids / 2) * 2 AS distinct_ids_bucket,
        distinct_ticket_ids + row_count AS distinct_ticket_ids_plus_row_count,
        last_event_at + INTERVAL 7 DAYS AS last_event_at_plus_7d
    FROM stats
    WHERE event_type = 'assigned'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(distinct_ticket_ids / NULLIF(SUM(distinct_ticket_ids) OVER w_distinct_ids_bucket_distinct, 0), 4) AS distinct_ticket_ids_share_of_distinct_ids_bucket,
        MAX(distinct_ticket_ids) OVER (PARTITION BY distinct_ids_bucket, distinct_ids) AS distinct_ids_bucket_distinct_ids_max_distinct_ti,
        RANK() OVER (ORDER BY row_count, ticket_id, actor, event_type) AS row_count_rank,
        SUM(distinct_ids) OVER (PARTITION BY distinct_ids_bucket, distinct_ids) AS distinct_ids_bucket_distinct_ids_sum_distinct_id,
        ROUND(PERCENT_RANK() OVER w_actor_by_distinct_ids, 3) AS distinct_ids_percent_rank,
        FIRST_VALUE(distinct_ticket_ids) OVER (ORDER BY row_count, ticket_id, actor, event_type ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_distinct_ticket_ids
    FROM enriched
    WINDOW w_distinct_ids_bucket_distinct AS (PARTITION BY distinct_ids_bucket, distinct_ids), w_actor_by_distinct_ids AS (PARTITION BY actor ORDER BY distinct_ids DESC, ticket_id, actor, event_type)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY distinct_ids, distinct_ids_bucket) AS rows_in_distinct_ids_distinct_ids_bucket,
        ROUND(distinct_ids_bucket - AVG(distinct_ids_bucket) OVER (PARTITION BY distinct_ids, distinct_ids_bucket), 2) AS distinct_ids_bucket_vs_distinct_ids_distinct_ids,
        first_distinct_ticket_ids - LAG(first_distinct_ticket_ids) OVER w_all_by_distinct_ids_bucket AS first_distinct_ticket_ids_change
    FROM ranked
    WINDOW w_all_by_distinct_ids_bucket AS (ORDER BY distinct_ids_bucket DESC, ticket_id, actor, event_type)
)
-- Final projection
SELECT
    ticket_id,
    actor,
    event_type,
    row_count,
    distinct_ids,
    last_event_at,
    distinct_ticket_ids_plus_row_count,
    last_event_at_plus_7d,
    distinct_ticket_ids_share_of_distinct_ids_bucket,
    distinct_ids_bucket_distinct_ids_max_distinct_ti,
    row_count_rank,
    distinct_ids_percent_rank,
    first_distinct_ticket_ids,
    rows_in_distinct_ids_distinct_ids_bucket,
    first_distinct_ticket_ids_change,
    SUBSTRING(TRIM(event_type), 1, 3) AS event_type_prefix
FROM sequenced
