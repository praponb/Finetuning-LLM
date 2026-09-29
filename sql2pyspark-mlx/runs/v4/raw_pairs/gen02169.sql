WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(device) AS device_upper
    FROM sessions
    WHERE started_at > TO_TIMESTAMP('2023-03-26 08:00:00')
      AND customer_id IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(country, TRUE) OVER w_part AS first_known_country,
        LAST_VALUE(browser) IGNORE NULLS OVER w_part AS last_known_browser
    FROM cleaned
    WINDOW w_part AS (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at, id) = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_grp AS seq_no,
        ROW_NUMBER() OVER w_grp AS event_seq,
        ROW_NUMBER() OVER w_grp AS seq_no_2
    FROM deduped
    WINDOW w_grp AS (ORDER BY started_at, id)
)
SELECT
    id,
    customer_id,
    device,
    browser,
    country,
    device_upper,
    first_known_country,
    last_known_browser,
    seq_no,
    CASE WHEN device RLIKE '^(desktop|mobile)$' THEN 'CORE' WHEN device RLIKE '^(tablet)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS device_zone
FROM metrics
ORDER BY device_zone NULLS LAST, id
