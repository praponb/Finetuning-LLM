WITH filtered AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (ticket_id IS NOT NULL OR HOUR(event_at) BETWEEN 8 AND 13)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER w_all_by_event_at_ties AS event_at_rank,
        NTILE(3) OVER (ORDER BY event_at DESC NULLS FIRST, id) AS event_at_tercile,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROW_NUMBER() OVER (ORDER BY event_at, id) AS seq_no,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        DENSE_RANK() OVER (ORDER BY event_at DESC NULLS FIRST) AS event_at_dense_rank,
        ROW_NUMBER() OVER w_all_by_event_at AS event_at_seq,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at
    FROM filtered
    WINDOW w_all_by_event_at_ties AS (ORDER BY event_at DESC), w_all_by_event_at AS (ORDER BY event_at DESC NULLS FIRST, id)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_rank,
        event_at_tercile,
        first_known_event_at,
        second_event_at,
        event_at_dense_rank,
        event_at_seq,
        first_event_at
    FROM sequenced
    WHERE seq_no = 1
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(first_event_at) OVER (ORDER BY event_at_dense_rank ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_first_event_at,
        MAX(event_at_dense_rank) OVER (ORDER BY event_at_dense_rank ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS event_at_dense_rank_max_3,
        COUNT(*) OVER w_event_at_tercile_event_at_se AS rows_in_event_at_tercile_event_at_seq,
        ROUND(event_at_seq - AVG(event_at_seq) OVER w_event_at_tercile_event_at_se, 2) AS event_at_seq_vs_event_at_tercile_event_at_seq_av,
        LAST_VALUE(first_event_at) OVER (PARTITION BY actor ORDER BY event_at_seq DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_first_event_at
    FROM leaders
    WINDOW w_event_at_tercile_event_at_se AS (PARTITION BY event_at_tercile, event_at_seq)
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        ticket_id,
        MAX(event_at_dense_rank_max_3) AS max_event_at_dense_rank_max_3,
        MAX(last_first_event_at) AS last_last_first_event_at,
        COUNT(*) AS cnt,
        SUM(event_at_rank) AS total_event_at_rank
    FROM with_trends
    WHERE ticket_id IS NOT NULL
    GROUP BY 1, 2
    HAVING MAX(event_at_tercile) > 2
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_last_last_first_event_at_mon, 3) AS cnt_cume_dist,
        RANK() OVER (PARTITION BY MONTH(last_last_first_event_at), MONTH(event_at_month) ORDER BY cnt) AS cnt_rank
    FROM grouped
    WINDOW w_last_last_first_event_at_mon AS (PARTITION BY MONTH(last_last_first_event_at), MONTH(event_at_month) ORDER BY cnt)
)
-- Final projection
SELECT
    event_at_month,
    max_event_at_dense_rank_max_3,
    last_last_first_event_at,
    cnt,
    total_event_at_rank,
    cnt_cume_dist,
    cnt_rank,
    ABS(cnt - 7) AS cnt_distance,
    DATEDIFF(DATE '2025-06-30', event_at_month) AS days_since_event_at_month
FROM with_windows
WHERE ticket_id IS NOT NULL
ORDER BY days_since_event_at_month DESC, cnt_rank
