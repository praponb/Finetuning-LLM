WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref,
        CASE WHEN actor IN ('customer', 'system') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        RANK() OVER (PARTITION BY actor ORDER BY event_at) AS event_at_rank,
        NTILE(10) OVER w_actor_by_event_at AS event_at_decile
    FROM valid
    WINDOW w_actor_by_event_at AS (PARTITION BY actor ORDER BY event_at, id)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(second_event_at, 'yyyy-MM-dd') AS second_event_at_label,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label
    FROM windowed
    WHERE ticket_id IS NOT NULL
),
deduped AS (
    -- Keep the latest row per event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_ref,
        actor_group,
        second_event_at,
        event_at_rank,
        event_at_decile,
        second_event_at_label,
        event_at_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_at_rank DESC, id DESC) AS recency_rank
        FROM derived
    ) ranked
    WHERE recency_rank = 1
)
SELECT id, metric, amount
FROM (
    SELECT id, event_at_decile, event_at_rank
    FROM deduped
) src
UNPIVOT (
    amount FOR metric IN (event_at_decile AS event_at_decile_v, event_at_rank AS event_at_rank_v)
) unp
ORDER BY id, metric
