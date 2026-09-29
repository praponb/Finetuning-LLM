WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time < '2024-10-22'
        )
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC NULLS LAST, id) AS latest_rank
        FROM kept
    ) t
    WHERE latest_rank = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        latest_rank,
        FIRST_VALUE(country, TRUE) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS event_seq,
        LAST_VALUE(browser, TRUE) OVER w_grp AS last_known_browser
    FROM ranked
    WINDOW w_grp AS (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM enriched
    QUALIFY RANK() OVER (PARTITION BY device ORDER BY event_seq) = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        latest_rank,
        first_known_country,
        last_known_browser,
        TO_DATE(started_at) AS started_at_date,
        TRIM(device) AS clean_device,
        COALESCE(browser, 'NONE') AS browser_clean
    FROM latest
    WHERE started_at >= TO_TIMESTAMP('2024-03-30 13:00:00')
      AND browser IS NOT NULL
),
cleaned_sessions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device,
        started_at,
        country,
        first_known_country,
        last_known_browser,
        started_at_date,
        browser_clean,
        LENGTH(country) AS country_length,
        DATE_ADD(started_at_date, 68) AS started_at_date_plus_68d
    FROM cleaned
    WHERE device IN ('mobile', 'tablet')
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        device,
        started_at,
        country,
        first_known_country,
        last_known_browser,
        started_at_date,
        browser_clean,
        country_length,
        started_at_date_plus_68d,
        LEAD(country_length, 1, 0) OVER (PARTITION BY country ORDER BY started_at_date, id) AS next_country_length,
        STDDEV_SAMP(country_length) OVER w_recent AS country_stddev_samp_country_length
    FROM cleaned_sessions
    WINDOW w_recent AS (PARTITION BY country)
)
-- Window metrics over several different partitions
SELECT
    id,
    device,
    started_at,
    country,
    first_known_country,
    last_known_browser,
    started_at_date,
    browser_clean,
    country_length,
    started_at_date_plus_68d,
    next_country_length,
    country_stddev_samp_country_length,
    AVG(next_country_length) OVER (PARTITION BY device ORDER BY next_country_length, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS next_country_length_avg_6,
    SUM(country_length) OVER (ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS country_length_sum_30d
FROM scored
