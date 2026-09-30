WITH enriched AS (
    SELECT
        id,
        customer_id,
        browser,
        LAST_VALUE(browser) IGNORE NULLS OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) AS event_seq,
        LAST_VALUE(country, TRUE) OVER (PARTITION BY MONTH(started_at), browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) AS seq_no,
        FIRST_VALUE(country, TRUE) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country
    FROM sessions
),
enriched_sessions AS (
    SELECT
        src.*,
        b.segment,
        b.country
    FROM enriched src
    JOIN customers b
        ON src.customer_id = b.id
),
kept AS (
    SELECT s.*
    FROM enriched_sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_type = 'page_view'
        )
),
filtered AS (
    SELECT
        id,
        browser,
        last_known_country,
        seq_no,
        first_known_country,
        segment,
        country,
        seq_no + event_seq AS seq_no_plus_event_seq,
        CONCAT_WS(',', browser, country) AS browser_country_key
    FROM kept
    WHERE customer_id IS NOT NULL
),
enriched_sessions_2 AS (
    SELECT
        *,
        LEAD(seq_no) OVER (PARTITION BY segment, browser ORDER BY seq_no_plus_event_seq, id) AS next_seq_no,
        LEAD(seq_no_plus_event_seq) OVER (PARTITION BY segment, browser ORDER BY seq_no_plus_event_seq, id) AS next_seq_no_plus_event_seq,
        LAG(seq_no_plus_event_seq) OVER (PARTITION BY segment, browser ORDER BY seq_no_plus_event_seq, id) AS prev_seq_no_plus_event_seq,
        ROW_NUMBER() OVER (PARTITION BY segment, browser ORDER BY seq_no_plus_event_seq, id) AS event_seq
    FROM filtered
),
latest AS (
    SELECT *
    FROM enriched_sessions_2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY segment, country ORDER BY next_seq_no DESC, id) = 1
)
SELECT
    CASE WHEN prev_seq_no_plus_event_seq >= 73 THEN 'LARGE' ELSE 'SMALL' END AS prev_seq_no_plus_event_seq_tier,
    country,
    CASE WHEN seq_no_plus_event_seq >= 65 THEN 'HIGH' ELSE 'LOW' END AS seq_no_plus_event_seq_tier,
    SUM(CASE WHEN segment = 'Consumer' THEN seq_no_plus_event_seq ELSE 0 END) AS consumer_seq_no_plus_event_seq,
    APPROX_PERCENTILE(seq_no_plus_event_seq, ARRAY(0.25, 0.5, 0.75)) AS seq_no_plus_event_seq_quartiles,
    PERCENTILE_APPROX(next_seq_no_plus_event_seq, ARRAY(0.25, 0.5, 0.75)) AS next_seq_no_plus_event_seq_quartiles,
    ROUND(SUM(next_seq_no_plus_event_seq) / NULLIF(COUNT(CASE WHEN browser = 'Firefox' THEN 1 END), 0), 2) AS next_seq_no_plus_event_seq_per_firefox
FROM latest
GROUP BY CASE WHEN prev_seq_no_plus_event_seq >= 73 THEN 'LARGE' ELSE 'SMALL' END, country, CASE WHEN seq_no_plus_event_seq >= 65 THEN 'HIGH' ELSE 'LOW' END
