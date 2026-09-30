WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        FLOOR(duration_sec) AS duration_sec_whole,
        CASE WHEN duration_sec >= 489 THEN 'High' WHEN duration_sec >= 114 THEN 'Medium' ELSE 'Low' END AS duration_sec_tier
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-03-25 06:00:00'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(duration_sec) OVER w_ordered AS running_sum_duration_sec,
        COUNT(duration_sec) OVER w_ordered AS running_count_duration_sec,
        MAX(duration_sec) OVER w_ordered AS running_max_duration_sec
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY session_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        a.id,
        a.session_id,
        a.page,
        a.event_time,
        a.duration_sec_tier,
        a.running_sum_duration_sec,
        a.running_count_duration_sec,
        p.browser,
        p.country
    FROM scored a
    LEFT JOIN sessions p
        ON a.session_id = p.id
),
joined AS (
    -- Attach sessions attributes
    SELECT src.*, d.device
    FROM with_dims src
    INNER JOIN sessions d
        ON src.session_id = d.id
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY browser ORDER BY running_count_duration_sec DESC NULLS LAST) AS latest_rank
        FROM joined
    ) t
    WHERE latest_rank = 1
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        country,
        CASE WHEN running_sum_duration_sec >= 2150 THEN 'HIGH' ELSE 'LOW' END AS running_sum_duration_sec_tier,
        MIN(running_count_duration_sec) AS min_running_count_duration_sec,
        SUM(running_sum_duration_sec) FILTER (WHERE device = 'desktop') AS desktop_total_running_sum_duration_sec,
        APPROX_PERCENTILE(running_sum_duration_sec, 0.5) AS running_sum_duration_sec_p50,
        MIN(event_time) AS first_seen,
        GROUPING_ID() AS grouping_id
    FROM latest
    GROUP BY CUBE (country, CASE WHEN running_sum_duration_sec >= 2150 THEN 'HIGH' ELSE 'LOW' END)
)
SELECT
    country,
    running_sum_duration_sec_tier,
    desktop_total_running_sum_duration_sec,
    running_sum_duration_sec_p50,
    first_seen,
    grouping_id,
    CASE WHEN country RLIKE '^(JP|TH)$' THEN 'GROUP_A' WHEN country RLIKE '^(US|SG)$' THEN 'CORE' ELSE 'EXTENDED' END AS country_zone
FROM stats;
