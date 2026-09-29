WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(event_type), 1, 2) AS event_type_prefix,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
    WHERE ticket_id >= 31
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_at,
        actor,
        event_type_len,
        MAX(event_type_len) OVER (PARTITION BY event_type_len, actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_event_type_len_so_far,
        SUM(event_type_len) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS event_type_len_sum_2,
        ROUND(event_type_len / NULLIF(SUM(event_type_len) OVER w_ticket_id, 0), 4) AS event_type_len_share_of_ticket_id,
        MAX(event_type_len) OVER (ORDER BY event_type_len, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS event_type_len_max_1_prior,
        MAX(event_type_len) OVER (PARTITION BY TO_DATE(event_at)) AS event_at_to_date_max_event_type_len,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROW_NUMBER() OVER (PARTITION BY ticket_id, event_type_len ORDER BY event_type_len DESC, id) AS seq_no
    FROM valid
    WINDOW w_ticket_id AS (PARTITION BY ticket_id)
),
top_rows AS (
    -- Top 5 per group
    SELECT
        id,
        event_at,
        actor,
        event_type_len,
        max_event_type_len_so_far,
        event_type_len_sum_2,
        event_type_len_share_of_ticket_id,
        event_type_len_max_1_prior,
        event_at_to_date_max_event_type_len,
        first_known_event_at
    FROM with_trends
    WHERE seq_no <= 5
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_at,
        event_type_len,
        event_type_len_share_of_ticket_id,
        event_type_len_max_1_prior,
        first_known_event_at,
        ROW_NUMBER() OVER (ORDER BY first_known_event_at, id) AS first_known_event_at_seq,
        NTH_VALUE(max_event_type_len_so_far, 2) OVER (PARTITION BY actor, event_type_len ORDER BY first_known_event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_max_event_type_len_so_far,
        ROUND(event_type_len_sum_2 - AVG(event_type_len_sum_2) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), actor), 2) AS event_type_len_sum_2_vs_event_at_month_actor_avg,
        ROUND(event_type_len / NULLIF(SUM(event_type_len) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), actor), 0), 4) AS event_type_len_share_of_event_at_month_actor,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), actor) AS rows_in_event_at_month_actor,
        SUM(max_event_type_len_so_far) OVER w_event_at_month_actor AS event_at_month_actor_sum_max_event_type_len_so_f
    FROM top_rows
    WINDOW w_event_at_month_actor AS (PARTITION BY DATE_TRUNC('MONTH', event_at), actor)
)
-- Final projection
SELECT
    id,
    event_at,
    event_type_len,
    event_type_len_share_of_ticket_id,
    event_type_len_max_1_prior,
    first_known_event_at,
    first_known_event_at_seq,
    second_max_event_type_len_so_far,
    rows_in_event_at_month_actor,
    event_at_month_actor_sum_max_event_type_len_so_f,
    event_at + INTERVAL 2 HOURS AS event_at_plus_2h
FROM ranked
WHERE rows_in_event_at_month_actor >= 9
ORDER BY id, second_max_event_type_len_so_far
