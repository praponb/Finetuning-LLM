WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        event_time,
        CONCAT_WS('/', event_type, page) AS event_type_page_key,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM web_events
    WHERE duration_sec >= 251
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(duration_sec_filled) OVER (ORDER BY event_time ASC, id ASC) AS prev_duration_sec_filled,
        ROW_NUMBER() OVER (ORDER BY event_time ASC, id ASC) AS row_seq
    FROM base
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(duration_sec_filled * 1.15, 2) AS duration_sec_filled_scaled
    FROM enriched
    WHERE row_seq > 4
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, duration_sec_filled * row_seq AS duration_sec_filled_times_row_seq
    FROM valid
    WHERE event_time >= TO_DATE('2023-08-27')
),
enriched_web_events AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        event_type_page_key,
        duration_sec_filled,
        duration_sec_filled_scaled,
        duration_sec_filled_times_row_seq,
        PERCENT_RANK() OVER (ORDER BY row_seq ASC NULLS LAST) AS row_seq_percent_rank,
        ROW_NUMBER() OVER (ORDER BY duration_sec_filled DESC, id ASC) AS row_seq_2,
        FIRST_VALUE(duration_sec_filled) IGNORE NULLS OVER (ORDER BY duration_sec_filled_scaled ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec_filled,
        FIRST_VALUE(duration_sec_filled_times_row_seq) IGNORE NULLS OVER (ORDER BY duration_sec_filled_times_row_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec_filled_times_row_seq,
        ROW_NUMBER() OVER (ORDER BY prev_duration_sec_filled DESC, id ASC) AS event_seq
    FROM filtered
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(event_seq) OVER (ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_seq,
        SUM(row_seq_2) OVER (ORDER BY row_seq_2 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_seq_2,
        NTILE(5) OVER (ORDER BY duration_sec_filled_times_row_seq DESC, id ASC) AS duration_sec_filled_times_row_seq_bucket
    FROM enriched_web_events
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(duration_sec_filled_scaled) OVER (ORDER BY duration_sec_filled ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS duration_sec_filled_scaled_avg_6,
        SUM(duration_sec_filled) OVER (ORDER BY row_seq_2 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec_filled
    FROM with_windows
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN duration_sec_filled_scaled >= 74 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_filled_scaled_tier,
    PERCENTILE_APPROX(running_event_seq, 0.9) AS running_event_seq_p90,
    PERCENTILE_APPROX(duration_sec_filled_times_row_seq, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_filled_times_row_seq_quartiles,
    PERCENTILE_APPROX(running_event_seq, ARRAY(0.25, 0.5, 0.75)) AS running_event_seq_quartiles,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(cumulative_duration_sec_filled, 0.95) AS cumulative_duration_sec_filled_p95,
    ROUND(AVG(event_seq), 2) AS avg_event_seq
FROM metrics
GROUP BY CASE WHEN duration_sec_filled_scaled >= 74 THEN 'LARGE' ELSE 'SMALL' END;
