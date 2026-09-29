WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE (actor NOT IN ('system', 'customer') OR event_type IN ('reopened', 'created'))
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        event_type_ref,
        RANK() OVER (PARTITION BY ticket_id ORDER BY event_at ASC) AS event_at_rank,
        ROW_NUMBER() OVER (ORDER BY event_at, id) AS seq_no,
        LAG(event_at) OVER (ORDER BY event_at, id) AS prev_event_at,
        ROW_NUMBER() OVER (PARTITION BY actor ORDER BY event_at, id) AS seq_no_2,
        LEAD(event_at) OVER w_all_by_event_at AS next_event_at,
        LEAD(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id) AS next_event_at_2,
        FIRST_VALUE(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at
    FROM scoped_events
    WINDOW w_all_by_event_at AS (ORDER BY event_at, id)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(event_type_ref), ' ', '_')) AS event_type_ref_slug,
        CASE WHEN seq_no >= 6 THEN 'large' WHEN seq_no >= 4 THEN 'medium' ELSE 'small' END AS seq_no_tier
    FROM ranked
    WHERE next_event_at < TIMESTAMP '2025-03-02 10:00:00'
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY prev_event_at ASC), 3) AS prev_event_at_percent_rank,
        MAX(seq_no) OVER w_event_type_seq_no AS event_type_seq_no_max_seq_no,
        SUM(seq_no_2) OVER (PARTITION BY seq_no_2, MONTH(next_event_at) ORDER BY last_known_event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no_2,
        NTH_VALUE(last_known_event_at, 2) OVER (ORDER BY first_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_known_event_at,
        MAX(event_at_rank) OVER w_event_type_seq_no AS event_type_seq_no_max_event_at_rank
    FROM enriched
    WINDOW w_event_type_seq_no AS (PARTITION BY event_type, seq_no)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    event_type_ref,
    event_at_rank,
    seq_no,
    prev_event_at,
    seq_no_2,
    next_event_at,
    next_event_at_2,
    first_event_at,
    last_known_event_at,
    event_type_ref_slug,
    seq_no_tier,
    prev_event_at_percent_rank,
    event_type_seq_no_max_seq_no,
    running_seq_no_2,
    event_type_seq_no_max_event_at_rank,
    COALESCE(running_seq_no_2, 0) AS running_seq_no_2_filled,
    SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix
FROM with_trends
ORDER BY running_seq_no_2_filled DESC
