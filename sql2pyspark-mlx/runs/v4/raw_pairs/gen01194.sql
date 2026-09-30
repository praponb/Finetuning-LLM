WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(event_type) AS clean_event_type,
        COALESCE(page, 'UNKNOWN') AS page_filled
    FROM web_events
    WHERE event_time >= TIMESTAMP '2023-09-03 18:00:00'
      AND page IS NOT NULL
      AND page IN ('/checkout', '/home', '/product')
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(duration_sec) OVER (ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS duration_sec_max_30d,
        SUM(duration_sec) OVER (PARTITION BY event_type ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS duration_sec_sum_7d,
        AVG(duration_sec) OVER (PARTITION BY session_id ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS duration_sec_avg_30d
    FROM cleaned
),
joined AS (
    -- Attach sessions attributes
    SELECT src.*, d.country
    FROM scored src
    JOIN sessions d
        ON src.session_id = d.id
),
scored_web_events AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        clean_event_type,
        page_filled,
        duration_sec_max_30d,
        duration_sec_sum_7d,
        duration_sec_avg_30d,
        country,
        ROW_NUMBER() OVER (ORDER BY duration_sec, id) AS row_seq,
        ROW_NUMBER() OVER (ORDER BY duration_sec_sum_7d DESC, id) AS seq_no,
        ROW_NUMBER() OVER w_part AS event_seq
    FROM joined
    WINDOW w_part AS (ORDER BY duration_sec_max_30d, id)
),
joined_web_events AS (
    -- Attach sessions attributes
    SELECT
        e.*,
        p.browser,
        p.device,
        p.country AS session_country
    FROM scored_web_events e
    LEFT JOIN sessions p
        ON e.session_id = p.id
),
enriched AS (
    -- Attach sessions attributes
    SELECT src.*, d.browser AS session_browser
    FROM joined_web_events src
    LEFT JOIN sessions d
        ON src.session_id = d.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        clean_event_type,
        page_filled,
        duration_sec_sum_7d,
        duration_sec_avg_30d,
        country,
        row_seq,
        seq_no,
        event_seq,
        browser,
        session_country,
        session_browser,
        CASE WHEN event_seq >= 7 THEN 'High' WHEN event_seq >= 3 THEN 'Medium' ELSE 'Low' END AS event_seq_band,
        ROUND(duration_sec_sum_7d * 0.9, 2) AS duration_sec_sum_7d_scaled
    FROM enriched
    WHERE browser IS NOT NULL
      AND duration_sec_max_30d BETWEEN 136 AND 299
)
SELECT
    id,
    session_id,
    clean_event_type,
    duration_sec_sum_7d,
    duration_sec_avg_30d,
    country,
    row_seq,
    seq_no,
    event_seq,
    CASE WHEN country RLIKE '^(JP|SG)$' THEN 'OTHER_ZONE' WHEN country RLIKE '^(TH|US)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS country_zone
FROM filtered
ORDER BY country NULLS FIRST, country_zone DESC;
