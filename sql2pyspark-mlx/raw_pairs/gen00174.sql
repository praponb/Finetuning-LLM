WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(duration_sec * 1.07, 2) AS duration_sec_adjusted,
        COALESCE(page, 'UNKNOWN') AS resolved_page
    FROM web_events
    WHERE event_time > TO_TIMESTAMP('2023-05-25 02:00:00')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(duration_sec_adjusted) OVER w_part AS prev_duration_sec_adjusted,
        LEAD(duration_sec) OVER w_part AS next_duration_sec,
        ROW_NUMBER() OVER w_part AS seq_no
    FROM cleaned
    WINDOW w_part AS (PARTITION BY page, session_id ORDER BY duration_sec_adjusted, id)
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        p2.browser,
        p2.country,
        p2.device
    FROM metrics s
    LEFT JOIN sessions p2
        ON s.session_id = p2.id
    WHERE p2.customer_id IN (12, 17)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        duration_sec,
        duration_sec_adjusted,
        resolved_page,
        next_duration_sec,
        seq_no,
        browser,
        device,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY prev_duration_sec_adjusted DESC, id) AS row_seq,
        ROW_NUMBER() OVER w_ordered AS seq_no_2,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(event_time), device ORDER BY seq_no, id) AS row_seq_2,
        STDDEV_SAMP(duration_sec_adjusted) OVER (PARTITION BY YEAR(event_time), page) AS event_time_year_stddev_samp_duration_sec_adjusted,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY event_time DESC, id) AS seq_no_3
    FROM with_dims
    WINDOW w_ordered AS (ORDER BY next_duration_sec, id)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        resolved_page,
        next_duration_sec,
        seq_no,
        device,
        row_seq,
        row_seq_2,
        event_time_year_stddev_samp_duration_sec_adjusted,
        FIRST_VALUE(device) IGNORE NULLS OVER (PARTITION BY device, browser ORDER BY next_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_device,
        LEAD(seq_no_3, 2, 0) OVER (PARTITION BY browser ORDER BY duration_sec, id) AS next_seq_no_3,
        SUM(duration_sec) OVER (PARTITION BY browser ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec
    FROM with_windows
),
base AS (
    -- Keep recent, valid rows
    SELECT *, CAST(event_time_year_stddev_samp_duration_sec_adjusted AS STRING) AS event_time_year_stddev_samp_duration_sec_adjusted_num
    FROM enriched
    WHERE device <> 'desktop'
)
-- Ranking and gap features
SELECT
    id,
    next_duration_sec,
    event_time_year_stddev_samp_duration_sec_adjusted,
    next_seq_no_3,
    event_time_year_stddev_samp_duration_sec_adjusted_num,
    LEAD(event_time_year_stddev_samp_duration_sec_adjusted) OVER w_grp AS next_event_time_year_stddev_samp_duration_sec_adjusted,
    LAG(event_time_year_stddev_samp_duration_sec_adjusted) OVER w_grp AS prev_event_time_year_stddev_samp_duration_sec_adjusted,
    LEAD(cumulative_duration_sec) OVER w_grp AS next_cumulative_duration_sec,
    ROW_NUMBER() OVER w_grp AS seq_no_2,
    PERCENT_RANK() OVER (PARTITION BY device ORDER BY next_seq_no_3 NULLS LAST) AS next_seq_no_3_percent_rank
FROM base
WINDOW w_grp AS (PARTITION BY device ORDER BY seq_no, id);
