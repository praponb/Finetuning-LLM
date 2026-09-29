WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(browser, '/', device) AS browser_device,
        SUBSTRING(country, 1, 4) AS country_prefix
    FROM sessions
    WHERE started_at > TO_DATE('2023-03-24')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(country) IGNORE NULLS OVER w_running AS last_known_country,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at, id) AS event_seq,
        LAST_VALUE(country, TRUE) OVER w_grp AS last_known_country_2,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS seq_no
    FROM valid
    WINDOW w_running AS (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY browser, device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(event_seq, 1) OVER (PARTITION BY browser ORDER BY event_seq, id) AS prev_event_seq,
        SUM(seq_no) OVER w_hist AS running_seq_no
    FROM with_windows
    WHERE event_seq > 8
    WINDOW w_hist AS (PARTITION BY browser, country ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    -- Keep recent, valid rows
    SELECT *, CEIL(seq_no) AS seq_no_whole
    FROM metrics
    WHERE country IS NOT NULL
      AND started_at > TO_DATE('2023-04-30')
)
-- Grouped metrics
SELECT
    running_seq_no,
    AVG(seq_no_whole) AS avg_seq_no_whole,
    SIZE(COLLECT_SET(country)) AS num_countries,
    SORT_ARRAY(COLLECT_SET(device)) AS device_list,
    PERCENTILE_APPROX(seq_no_whole, 0.9) AS seq_no_whole_p90
FROM base
GROUP BY running_seq_no
