WITH recent_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 30 MINUTES AS event_at_plus_30m,
        DATE_TRUNC('MONTH', event_at) AS event_at_month
    FROM ticket_events
    WHERE actor <> 'customer'
      AND event_type IS NOT NULL
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        event_type,
        event_at_month,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM recent_events
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(event_at_month, 'yyyy-MM') AS event_at_month_ym,
        SUM(CASE WHEN event_type IN ('resolved', 'assigned') THEN 1 ELSE 0 END) AS n_event_type_in_resolved_assig,
        COUNT(*) FILTER (WHERE event_type IN ('replied', 'created', 'reopened')) AS n_event_type_in_replied_create,
        MIN(event_at_month) AS first_event_at_month
    FROM enriched
    GROUP BY 1
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE n_event_type_in_resolved_assig > (SELECT AVG(n_event_type_in_resolved_assig) FROM rollup_base)
),
annotated AS (
    -- Derive helper columns
    SELECT
        event_at_month_ym,
        n_event_type_in_replied_create,
        SUBSTRING(TRIM(event_at_month_ym), 1, 3) AS event_at_month_ym_prefix
    FROM screened
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER w_n_event_type_in_replied_crea AS n_event_type_in_replied_create_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY n_event_type_in_replied_create ORDER BY n_event_type_in_replied_create DESC), 3) AS n_event_type_in_replied_create_cume_dist,
        NTILE(10) OVER (ORDER BY n_event_type_in_replied_create ASC NULLS LAST, event_at_month_ym) AS n_event_type_in_replied_create_decile,
        DENSE_RANK() OVER (ORDER BY n_event_type_in_replied_create ASC NULLS LAST, event_at_month_ym) AS n_event_type_in_replied_create_dense_rank_2
    FROM annotated
    WINDOW w_n_event_type_in_replied_crea AS (PARTITION BY n_event_type_in_replied_create ORDER BY n_event_type_in_replied_create)
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM metrics
    WHERE n_event_type_in_replied_create_dense_rank > (SELECT AVG(n_event_type_in_replied_create_dense_rank) FROM metrics)
)
SELECT
    event_at_month_ym,
    n_event_type_in_replied_create,
    event_at_month_ym_prefix,
    n_event_type_in_replied_create_dense_rank,
    n_event_type_in_replied_create_cume_dist,
    n_event_type_in_replied_create_decile,
    n_event_type_in_replied_create_dense_rank_2
FROM without_matches
