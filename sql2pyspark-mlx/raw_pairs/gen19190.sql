WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        CASE WHEN duration_sec >= 421 THEN 'High' WHEN duration_sec >= 141 THEN 'Medium' ELSE 'Low' END AS duration_sec_band
    FROM web_events
    WHERE event_time >= DATE '2023-08-20'
      AND page IS NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC NULLS FIRST, id) = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        duration_sec_band,
        NTILE(5) OVER (PARTITION BY event_type ORDER BY duration_sec, id) AS duration_sec_bucket,
        ROW_NUMBER() OVER (ORDER BY duration_sec, id) AS event_seq,
        LEAD(duration_sec, 2) OVER (PARTITION BY page ORDER BY duration_sec, id) AS next_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY event_type ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS duration_sec_sum_7d,
        AVG(duration_sec) OVER (PARTITION BY YEAR(event_time) ORDER BY duration_sec, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS duration_sec_avg_3
    FROM latest
    WHERE TO_DATE(event_time) < '2023-08-01'
),
latest_web_events AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec_avg_3 NULLS FIRST, id) AS rn
        FROM metrics
    ) t
    WHERE rn = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        session_id,
        event_time,
        next_duration_sec,
        rn,
        LAST_VALUE(event_seq) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_seq,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY YEAR(event_time)), 4) AS event_seq_share,
        ROW_NUMBER() OVER (ORDER BY event_seq DESC, id) AS seq_no
    FROM latest_web_events
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        a.*,
        p.country,
        p.device
    FROM with_windows a
    LEFT JOIN sessions p
        ON a.session_id = p.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, seq_no - rn AS seq_no_minus_rn
    FROM enriched
    WHERE country IS DISTINCT FROM 'DE'
      AND next_duration_sec = 431
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            session_id,
            country,
            next_duration_sec
        FROM filtered
    ) src
    PIVOT (
        SUM(next_duration_sec) AS value_sum,
        COUNT(1) AS cnt
        FOR country IN ('TH', 'JP')
    )
)
SELECT
    s.session_id,
    s.TH_value_sum,
    s.JP_value_sum,
    s.JP_cnt,
    (SELECT MAX(w2.duration_sec) FROM web_events w2 WHERE w2.session_id = s.session_id) AS max_web_event_duration_sec
FROM wide s
ORDER BY TH_value_sum;
