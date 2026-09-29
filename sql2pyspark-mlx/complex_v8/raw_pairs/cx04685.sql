WITH valid_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref,
        MONTH(event_at) AS event_at_month,
        DATE_TRUNC('HOUR', event_at) AS event_at_hour
    FROM ticket_events
    WHERE ticket_id <= 18
      AND event_type IN ('resolved', 'assigned')
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(5) OVER w_event_at_month_ticket_id_by AS event_at_hour_quintile,
        RANK() OVER (PARTITION BY MONTH(event_at), ticket_id ORDER BY event_at_hour DESC) AS event_at_hour_rank,
        SUM(event_at_month) OVER (PARTITION BY MONTH(event_at), ticket_id ORDER BY event_at_hour DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS event_at_month_sum_1_prior
    FROM valid_events
    WINDOW w_event_at_month_ticket_id_by AS (PARTITION BY MONTH(event_at), ticket_id ORDER BY event_at_hour DESC, id)
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        actor,
        actor_ref,
        event_at_month,
        event_at_hour_quintile,
        event_at_month_sum_1_prior,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM with_trends
    WHERE event_at < TIMESTAMP '2025-02-18 11:00:00'
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(event_at_hour_quintile / 2) * 2 AS event_at_hour_quintile_bucket,
        GREATEST(event_at_month, event_at_hour_quintile) AS greatest_event_at_month_event_at_hour_quintile,
        LEAST(event_at_hour_quintile, event_at_month) AS least_event_at_hour_quintile_event_at_month
    FROM derived
    WHERE event_type_or_unknown <> 'reopened'
),
top_rows AS (
    -- Top 2 per group
    SELECT *
    FROM labeled
    WHERE event_at_hour_rank <= 2
      AND event_at_hour_quintile_bucket < 7
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        event_at_month,
        event_at_hour_quintile,
        event_at_month_sum_1_prior,
        event_type_or_unknown,
        greatest_event_at_month_event_at_hour_quintile,
        least_event_at_hour_quintile_event_at_month,
        ABS(event_at_month - 6) AS event_at_month_distance,
        LENGTH(TRIM(actor_ref)) AS actor_ref_len
    FROM top_rows
)
SELECT
    id,
    actor_ref_len,
    FLOOR(actor_ref_len / 2) * 2 AS actor_ref_len_bucket,
    LENGTH(TRIM(event_type_or_unknown)) AS event_type_or_unknown_len
FROM scored
WHERE event_at_month >= 3
ORDER BY id DESC, actor_ref_len DESC NULLS FIRST
