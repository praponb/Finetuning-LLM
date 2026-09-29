WITH base_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-25 08:00:00'
      AND event_type = 'created'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(10) OVER w_all_by_event_type_len AS event_type_len_decile,
        ROUND(event_type_len / NULLIF(SUM(event_type_len) OVER (PARTITION BY event_type), 0), 4) AS event_type_len_share_of_event_type,
        MIN(event_type_len) OVER w_event_type AS event_type_min_event_type_len,
        ROUND(AVG(event_type_len) OVER (ORDER BY event_type_len, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS event_type_len_avg_3,
        ROUND(AVG(event_type_len) OVER (PARTITION BY event_type), 2) AS event_type_avg_event_type_len,
        COUNT(*) OVER (PARTITION BY event_type) AS rows_in_event_type,
        MAX(event_type_len) OVER (PARTITION BY event_type) AS event_type_max_event_type_len,
        MAX(event_type_len) OVER (PARTITION BY event_type_len ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_event_type_len_so_far,
        RANK() OVER (PARTITION BY event_type_len ORDER BY event_at, id) AS event_at_rank
    FROM base_events
    WINDOW w_all_by_event_type_len AS (ORDER BY event_type_len, id), w_event_type AS (PARTITION BY event_type)
),
annotated AS (
    -- Add calculated columns
    SELECT *, CONCAT_WS('-', event_type, actor) AS event_type_actor_label
    FROM with_trends
    WHERE event_type_len_share_of_event_type < 0.7
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(rows_in_event_type) OVER (PARTITION BY event_type ORDER BY event_at, id) AS next_rows_in_event_type,
        ROUND(AVG(event_type_len) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS event_type_len_avg_3_prior,
        ROUND(event_type_len_avg_3 - AVG(event_type_len_avg_3) OVER (PARTITION BY event_type_len), 2) AS event_type_len_avg_3_vs_event_type_len_avg,
        MIN(event_type_len) OVER (PARTITION BY event_type_len) AS event_type_len_min_event_type_len,
        COUNT(*) OVER (PARTITION BY event_type_len) AS rows_in_event_type_len,
        SIZE(COLLECT_SET(ticket_id) OVER w_event_type_len) AS distinct_ticket_id_in_event_type_len
    FROM annotated
    WINDOW w_event_type_len AS (PARTITION BY event_type_len)
),
totals AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        event_at_rank,
        SUM(event_type_len_share_of_event_type) AS total_event_type_len_share_of_event_type,
        ROUND(SUM(event_type_len_avg_3_prior) / NULLIF(SUM(event_type_len_avg_3), 0), 3) AS event_type_len_avg_3_prior_to_event_type_len_avg,
        COUNT(*) AS cnt
    FROM metrics
    WHERE event_type_len_avg_3_prior < 12.2
    GROUP BY 1, 2
)
-- Final projection
SELECT
    event_at_month,
    event_at_rank,
    event_type_len_avg_3_prior_to_event_type_len_avg,
    cnt
FROM totals
WHERE HOUR(event_at_month) BETWEEN 8 AND 11
ORDER BY event_at_rank DESC
