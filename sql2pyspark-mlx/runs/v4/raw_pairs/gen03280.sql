WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        FIRST_VALUE(country, TRUE) OVER w_grp AS first_known_country,
        ROW_NUMBER() OVER (ORDER BY started_at, id) AS seq_no
    FROM sessions
    WINDOW w_grp AS (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_sessions AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(seq_no) OVER (PARTITION BY browser ORDER BY seq_no, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS seq_no_avg_5_prior,
        ROW_NUMBER() OVER (PARTITION BY customer_id, country ORDER BY started_at, id) AS event_seq
    FROM with_windows
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device,
        browser,
        started_at,
        country,
        first_known_country,
        event_seq,
        ROUND(seq_no_avg_5_prior, 1) AS seq_no_avg_5_prior_rounded,
        COALESCE(browser, 'DIRECT') AS resolved_browser
    FROM with_windows_sessions
    WHERE browser NOT IN ('Firefox', 'Chrome')
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY device ORDER BY seq_no_avg_5_prior_rounded NULLS LAST) AS row_num
        FROM cleaned
    ) x
    WHERE row_num = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time >= '2023-10-01'
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        device,
        browser,
        started_at,
        country,
        first_known_country,
        event_seq,
        seq_no_avg_5_prior_rounded,
        resolved_browser,
        row_num,
        LAST_VALUE(seq_no_avg_5_prior_rounded, TRUE) OVER w_recent AS last_known_seq_no_avg_5_prior_rounded,
        SUM(event_seq) OVER w_hist AS cumulative_event_seq,
        CUME_DIST() OVER (PARTITION BY device ORDER BY event_seq DESC NULLS LAST) AS event_seq_cume_dist
    FROM eligible
    WINDOW w_recent AS (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY browser ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_sessions_2 AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(last_known_seq_no_avg_5_prior_rounded) OVER w_recent AS last_known_seq_no_avg_5_prior_rounded_count_7d,
        DENSE_RANK() OVER (PARTITION BY country, browser ORDER BY event_seq DESC NULLS LAST) AS event_seq_dense_rank,
        COUNT(event_seq_cume_dist) OVER w_running AS event_seq_cume_dist_count_90d
    FROM metrics
    WINDOW w_recent AS (ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY country ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', started_at) AS year_start,
    device,
    CASE WHEN event_seq_dense_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END AS event_seq_dense_rank_tier,
    MIN(started_at) AS first_seen,
    PERCENTILE_APPROX(last_known_seq_no_avg_5_prior_rounded_count_7d, 0.9) AS last_known_seq_no_avg_5_prior_rounded_count_7d_p90,
    MAX(started_at) AS last_started_at
FROM with_windows_sessions_2
GROUP BY ROLLUP (DATE_TRUNC('YEAR', started_at), device, CASE WHEN event_seq_dense_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END)
HAVING last_known_seq_no_avg_5_prior_rounded_count_7d_p90 < 0
