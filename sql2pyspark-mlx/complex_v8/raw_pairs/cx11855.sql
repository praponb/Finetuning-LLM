WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref
    FROM ticket_events
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_date,
        actor_ref,
        ROW_NUMBER() OVER w_event_at_date_month_event_ty AS rn,
        ROUND(CUME_DIST() OVER w_ticket_id_event_type_by_even, 3) AS event_at_date_cume_dist,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', event_at_date), event_type ORDER BY event_at) AS event_at_rank,
        LAG(event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at_date), event_type ORDER BY event_at, id) AS prev_event_at,
        RANK() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at_date) AS event_at_date_rank,
        DATEDIFF(event_at_date, LAG(event_at_date) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at_date, id)) AS days_since_prev_event_at_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at_date, id), 3) AS event_at_date_percent_rank,
        ROUND(PERCENT_RANK() OVER w_ticket_id_event_type_by_even, 3) AS event_at_date_percent_rank_2,
        LAG(event_at, 3) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at_date, id) AS prev3_event_at
    FROM prepared
    WINDOW w_event_at_date_month_event_ty AS (PARTITION BY DATE_TRUNC('MONTH', event_at_date), event_type ORDER BY event_at, id), w_ticket_id_event_type_by_even AS (PARTITION BY ticket_id, event_type ORDER BY event_at_date)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        GREATEST(rn, event_at_date_rank) AS greatest_rn_event_at_date_rank,
        CASE WHEN event_at_date_rank >= 9 THEN 'HIGH' WHEN event_at_date_rank >= 4 THEN 'MEDIUM' ELSE 'LOW' END AS event_at_date_rank_tier,
        LOWER(TRIM(actor_ref)) AS actor_ref_clean
    FROM with_windows
),
per_group AS (
    -- Summarise per group
    SELECT
        TO_DATE(prev3_event_at) AS prev3_event_at_day,
        event_at_date_rank_tier,
        ROUND(SUM(greatest_rn_event_at_date_rank) / NULLIF(SUM(event_at_date_percent_rank), 0), 3) AS greatest_rn_event_at_date_rank_to_event_at_date_,
        COUNT(DISTINCT ticket_id) AS distinct_ticket_ids,
        COUNT(*) AS cnt
    FROM tagged
    GROUP BY 1, 2
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY distinct_ticket_ids ORDER BY cnt DESC, prev3_event_at_day, event_at_date_rank_tier) AS cnt_decile,
        NTILE(5) OVER w_distinct_ticket_ids_by_cnt AS cnt_quintile
    FROM per_group
    WINDOW w_distinct_ticket_ids_by_cnt AS (PARTITION BY distinct_ticket_ids ORDER BY cnt DESC, prev3_event_at_day, event_at_date_rank_tier)
)
SELECT
    prev3_event_at_day,
    event_at_date_rank_tier,
    greatest_rn_event_at_date_rank_to_event_at_date_,
    distinct_ticket_ids,
    cnt_quintile,
    FLOOR(cnt / 2) * 2 AS cnt_bucket,
    CASE WHEN distinct_ticket_ids >= 7 THEN 'HIGH' WHEN distinct_ticket_ids >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS distinct_ticket_ids_tier
FROM with_trends
WHERE distinct_ticket_ids >= 2
ORDER BY event_at_date_rank_tier DESC NULLS FIRST, prev3_event_at_day
LIMIT 5
