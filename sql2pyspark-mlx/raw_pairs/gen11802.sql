WITH cleaned AS (
    SELECT
        id,
        session_id,
        page,
        duration_sec,
        CAST(event_time AS DATE) AS event_time_day,
        COALESCE(page, 'UNKNOWN') AS page_filled
    FROM web_events
    WHERE page IS NOT NULL
      AND session_id IN (21, 29, 39, 40)
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY page ORDER BY duration_sec DESC NULLS LAST) AS rn
    FROM cleaned
    QUALIFY rn = 1
),
metrics AS (
    SELECT
        *,
        LEAD(duration_sec, 1) OVER (ORDER BY event_time_day ASC, id ASC) AS next_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id ASC) AS seq_no,
        NTILE(5) OVER w_recent AS duration_sec_bucket
    FROM ranked
    WINDOW w_recent AS (ORDER BY duration_sec DESC, id ASC)
),
latest AS (
    SELECT
        id,
        page,
        duration_sec
    FROM (
        SELECT *, RANK() OVER (PARTITION BY session_id ORDER BY seq_no DESC) AS rn_2
        FROM metrics
    ) x
    WHERE rn_2 = 1
),
scored AS (
    SELECT
        id,
        page,
        LAG(duration_sec, 2) OVER w_part AS prev_duration_sec,
        RANK() OVER w_part_2 AS duration_sec_rank,
        AVG(duration_sec) OVER (ORDER BY duration_sec ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS duration_sec_avg_3,
        NTILE(10) OVER w_part AS duration_sec_bucket,
        NTILE(10) OVER w_grp AS duration_sec_bucket_2
    FROM latest
    WINDOW w_part AS (ORDER BY duration_sec ASC, id ASC), w_part_2 AS (PARTITION BY page ORDER BY duration_sec DESC NULLS LAST), w_grp AS (PARTITION BY page ORDER BY duration_sec DESC, id ASC)
),
report AS (
    SELECT
        id,
        page,
        duration_sec_bucket,
        duration_sec_bucket_2
    FROM scored
    ORDER BY page ASC NULLS LAST
)
SELECT
    id,
    page,
    duration_sec_bucket,
    duration_sec_bucket_2
FROM report
