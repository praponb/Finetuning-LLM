WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        ROUND(duration_sec, 1) AS duration_sec_rounded,
        TRIM(page) AS clean_page,
        COALESCE(page, 'N/A') AS page_clean
    FROM web_events
    WHERE page IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        session_id,
        duration_sec_rounded
    FROM cleaned
    QUALIFY RANK() OVER (PARTITION BY event_type ORDER BY duration_sec) = 1
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        src.id,
        src.session_id,
        src.duration_sec_rounded,
        d.country,
        d.device
    FROM deduped src
    LEFT JOIN sessions d
        ON src.session_id = d.id
    WHERE d.country IN ('UK', 'JP', 'SG')
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        duration_sec_rounded,
        LEAD(duration_sec_rounded) OVER (PARTITION BY session_id, country ORDER BY duration_sec_rounded, id) AS next_duration_sec_rounded,
        ROW_NUMBER() OVER (PARTITION BY session_id, country ORDER BY duration_sec_rounded, id) AS seq_no
    FROM enriched
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN duration_sec_rounded >= 72 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_rounded_tier,
    PERCENTILE_APPROX(next_duration_sec_rounded, 0.95) AS next_duration_sec_rounded_p95,
    PERCENTILE_APPROX(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles,
    APPROX_PERCENTILE(seq_no, 0.5) AS seq_no_p50,
    APPROX_PERCENTILE(next_duration_sec_rounded, 0.5) AS next_duration_sec_rounded_p50,
    ROUND(AVG(next_duration_sec_rounded), 2) AS avg_next_duration_sec_rounded
FROM scored
GROUP BY CASE WHEN duration_sec_rounded >= 72 THEN 'PREMIUM' ELSE 'STANDARD' END
