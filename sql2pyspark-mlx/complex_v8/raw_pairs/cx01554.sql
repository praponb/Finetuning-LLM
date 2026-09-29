WITH valid_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE event_type IN ('reopened', 'resolved', 'replied')
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at
    FROM valid_events s
),
latest AS (
    -- Keep the latest row per ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_date,
        event_type_slug,
        days_since_event_at,
        event_count,
        last_event_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY days_since_event_at DESC, id DESC) AS rn
        FROM summarized_rows
    ) ranked
    WHERE rn = 1
),
stats AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(event_at_date, 'yyyy-MM') AS event_at_date_ym,
        COUNT(*) AS n_rows,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        MAX(event_at_date) AS last_event_at_date,
        MIN(days_since_event_at) AS min_days_since_event_at
    FROM latest
    GROUP BY 1
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE n_rows > (SELECT AVG(n_rows) FROM stats)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY n_rows ORDER BY n_rows, event_at_date_ym) AS n_rows_tercile,
        ROUND(AVG(min_days_since_event_at) OVER (PARTITION BY YEAR(last_event_at_date), n_rows), 2) AS last_event_at_date_year_n_rows_avg_min_days_sinc
    FROM without_matches
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(n_rows_tercile - AVG(n_rows_tercile) OVER w_last_event_at_date_month, 2) AS n_rows_tercile_vs_last_event_at_date_month_avg,
        RANK() OVER (PARTITION BY n_rows_tercile ORDER BY last_event_at_date, event_at_date_ym) AS last_event_at_date_rank,
        SUM(n_rows) OVER (PARTITION BY MONTH(last_event_at_date) ORDER BY last_event_at_date_year_n_rows_avg_min_days_sinc DESC, event_at_date_ym ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS n_rows_sum_6_prior
    FROM with_trends
    WINDOW w_last_event_at_date_month AS (PARTITION BY DATE_TRUNC('MONTH', last_event_at_date))
)
-- Final projection
SELECT
    event_at_date_ym,
    n_rows,
    actor_list,
    last_event_at_date,
    min_days_since_event_at,
    n_rows_tercile,
    last_event_at_date_year_n_rows_avg_min_days_sinc,
    n_rows_tercile_vs_last_event_at_date_month_avg,
    n_rows_sum_6_prior
FROM ranked
WHERE n_rows_sum_6_prior <= 29
ORDER BY n_rows_sum_6_prior DESC, n_rows_tercile DESC
