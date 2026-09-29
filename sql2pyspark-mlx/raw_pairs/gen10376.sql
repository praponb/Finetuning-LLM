WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN device = 'mobile' THEN 1 ELSE 0 END AS is_mobile,
        COALESCE(country, 'DIRECT') AS country_filled
    FROM sessions
    WHERE started_at > TIMESTAMP '2023-07-30 16:00:00'
      AND country IS NOT NULL
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.page <> '/checkout'
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        country_filled,
        LEAD(is_mobile) OVER (PARTITION BY customer_id ORDER BY started_at, id) AS next_is_mobile,
        LAG(is_mobile) OVER (PARTITION BY customer_id ORDER BY started_at, id) AS prev_is_mobile,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) AS seq_no,
        SUM(is_mobile) OVER (PARTITION BY DATE_TRUNC('YEAR', started_at), country ORDER BY is_mobile, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_is_mobile
    FROM kept
    WHERE is_mobile = 31
),
enriched_sessions AS (
    -- Attach customers attributes
    SELECT
        e.*,
        d.name,
        d.city
    FROM enriched e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device,
        browser,
        country,
        country_filled,
        prev_is_mobile,
        seq_no,
        running_is_mobile,
        city,
        MIN(prev_is_mobile) OVER w_part AS running_min_prev_is_mobile,
        MAX(prev_is_mobile) OVER w_part AS running_max_prev_is_mobile,
        AVG(prev_is_mobile) OVER w_part AS running_avg_prev_is_mobile,
        SUM(prev_is_mobile) OVER w_part AS running_sum_prev_is_mobile,
        LEAD(running_is_mobile, 2) OVER (PARTITION BY city ORDER BY next_is_mobile, id) AS next_running_is_mobile
    FROM enriched_sessions
    WINDOW w_part AS (PARTITION BY browser ORDER BY running_is_mobile, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(running_max_prev_is_mobile) OVER (PARTITION BY city, country) AS city_avg_running_max_prev_is_mobile,
        STDDEV_SAMP(running_sum_prev_is_mobile) OVER (PARTITION BY device, browser) AS device_stddev_samp_running_sum_prev_is_mobile
    FROM metrics
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM with_windows
    QUALIFY RANK() OVER (PARTITION BY city, device ORDER BY seq_no) = 1
)
SELECT
    id,
    browser,
    country_filled,
    prev_is_mobile,
    seq_no,
    running_min_prev_is_mobile,
    running_max_prev_is_mobile,
    running_avg_prev_is_mobile,
    running_sum_prev_is_mobile
FROM latest
ORDER BY running_sum_prev_is_mobile DESC
