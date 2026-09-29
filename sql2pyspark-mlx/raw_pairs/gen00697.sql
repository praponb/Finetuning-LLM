WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(country, 1, 3) country_prefix,
        COALESCE(browser, 'UNKNOWN') browser_clean
    FROM sessions
    WHERE started_at > TO_TIMESTAMP('2023-03-22 11:00:00')
      AND customer_id IN (1, 2)
),
joined AS (
    -- Attach customers attributes
    SELECT src.*, d.age
    FROM cleaned src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY age NULLS FIRST, id) AS dedup_rank
        FROM joined
    ) x
    WHERE dedup_rank = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device,
        browser,
        country_prefix,
        browser_clean,
        age,
        dedup_rank,
        LEAD(age) OVER (PARTITION BY customer_id ORDER BY age, id) next_age,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY age, id) seq_no,
        MAX(age) OVER (PARTITION BY TO_DATE(started_at), customer_id) started_at_to_date_max_age
    FROM deduped
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.duration_sec = 434
        )
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.device,
        s.browser,
        s.country_prefix,
        s.browser_clean,
        s.age,
        s.dedup_rank,
        s.next_age,
        x.web_event_event_type
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY browser, device ORDER BY next_age DESC NULLS LAST, id) AS row_num
    FROM with_latest
    QUALIFY row_num = 1
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        web_event_event_type,
        browser,
        age
    FROM latest
) src
PIVOT (
    SUM(age) AS value_sum,
    COUNT(1) AS n
    FOR browser IN ('Firefox', 'Edge', 'Safari')
)
