WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        INITCAP(TRIM(event_type)) AS event_type_title,
        CAST(duration_sec AS DOUBLE) AS duration_sec_num,
        COALESCE(page, 'DIRECT') AS resolved_page
    FROM web_events
    WHERE event_time > DATE '2023-05-24'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(duration_sec_num) OVER (PARTITION BY page ORDER BY duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec_num,
        AVG(duration_sec_num) OVER (ORDER BY event_time ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS duration_sec_num_avg_2_prior,
        ROW_NUMBER() OVER w_part AS event_seq
    FROM filtered
    WINDOW w_part AS (PARTITION BY event_type ORDER BY duration_sec_num DESC, id ASC)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        page,
        duration_sec,
        event_type_title,
        duration_sec_num,
        resolved_page,
        cumulative_duration_sec_num,
        duration_sec_num_avg_2_prior,
        CAST(event_time AS DATE) AS event_time_day,
        COALESCE(page, 'NONE') AS page_clean
    FROM enriched
    WHERE event_time >= DATE '2023-09-30'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY session_id ORDER BY duration_sec_num_avg_2_prior ASC, id ASC) AS duration_sec_num_avg_2_prior_bucket,
        SUM(duration_sec) OVER (PARTITION BY page ORDER BY event_time_day ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec
    FROM cleaned
    WHERE duration_sec <= 402
),
enriched_web_events AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        p.device,
        p.browser
    FROM with_windows s
    LEFT JOIN sessions p
        ON s.session_id = p.id
),
enriched_web_events_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(duration_sec_num) OVER (PARTITION BY MONTH(event_time_day), browser ORDER BY event_time_day ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec_num,
        CUME_DIST() OVER (PARTITION BY device ORDER BY duration_sec_num_avg_2_prior_bucket ASC) AS duration_sec_num_avg_2_prior_bucket_cume_dist,
        ROUND(duration_sec_num_avg_2_prior_bucket / SUM(duration_sec_num_avg_2_prior_bucket) OVER (PARTITION BY device, browser), 4) AS duration_sec_num_avg_2_prior_bucket_share
    FROM enriched_web_events
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        session_id,
        device,
        browser,
        running_duration_sec_num
    FROM enriched_web_events_2
) src
PIVOT (
    SUM(running_duration_sec_num) AS amount
    FOR browser IN ('Chrome', 'Firefox')
)
