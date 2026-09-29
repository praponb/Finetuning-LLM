WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        TO_DATE(event_time) AS event_time_date,
        COALESCE(page, 'DIRECT') AS page_filled
    FROM web_events
    WHERE event_time > TO_DATE('2024-03-13')
      AND page IS NOT NULL
      AND event_type NOT IN ('click', 'search', 'add_to_cart')
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN event_type IN ('search', 'page_view') THEN 'Tier 1' ELSE 'Other' END AS event_type_group,
        DATEDIFF(CURRENT_DATE(), event_time) AS days_since_event_time
    FROM valid
    WHERE event_time >= DATE '2024-03-02'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM filtered
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY event_type ORDER BY duration_sec DESC NULLS FIRST) AS rn
    FROM sessionized
    QUALIFY rn = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(new_group_flag) OVER w_grp AS new_group_flag_avg_6_prior,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec, id) AS event_seq
    FROM deduped
    WINDOW w_grp AS (PARTITION BY session_id, event_type ORDER BY event_time_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
deduped_web_events AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type, session_id ORDER BY new_group_flag NULLS FIRST, id) AS rn_2
        FROM with_windows
    ) t
    WHERE rn_2 = 1
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN new_group_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS new_group_flag_tier,
        COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS add_to_cart_rows,
        COUNT(*) FILTER (WHERE event_type = 'click') AS click_rows,
        SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list
    FROM deduped_web_events
    GROUP BY CASE WHEN new_group_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END
    HAVING COUNT(*) FILTER (WHERE event_type = 'click') > 1
)
SELECT
    new_group_flag_tier,
    add_to_cart_rows,
    click_rows,
    event_type_list
FROM stats
ORDER BY new_group_flag_tier DESC
