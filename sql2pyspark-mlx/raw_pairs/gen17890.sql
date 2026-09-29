WITH eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM customers s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.customer_id IN (12, 36)
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.order_date <= s.signup_date
        )
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(age * 1.15, 2) AS age_scaled,
        COALESCE(city, 'UNKNOWN') AS resolved_city
    FROM kept
    WHERE signup_date >= DATE '2023-03-22'
      AND segment IS NOT NULL
),
latest_match AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.session_country
    FROM base s
    JOIN LATERAL (
            SELECT c.country AS session_country
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        name,
        segment,
        is_active,
        resolved_city,
        LAST_VALUE(session_country) IGNORE NULLS OVER (PARTITION BY YEAR(signup_date), segment ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_session_country,
        SUM(age_scaled) OVER (PARTITION BY segment ORDER BY age_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_age_scaled,
        ROW_NUMBER() OVER (PARTITION BY country, session_country ORDER BY credit_limit, id) AS event_seq
    FROM latest_match
    WHERE YEAR(signup_date) = 2024
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY segment ORDER BY event_seq DESC) AS latest_rank
    FROM scored
    QUALIFY latest_rank = 1
)
-- Grouped metrics
SELECT
    segment,
    ROUND(SUM(running_age_scaled) / NULLIF(COUNT(CASE WHEN segment = 'Corporate' THEN 1 END), 0), 2) AS running_age_scaled_per_corporate,
    SUM(CASE WHEN segment = 'SMB' THEN event_seq ELSE 0 END) AS smb_event_seq,
    APPROX_PERCENTILE(running_age_scaled, ARRAY(0.25, 0.5, 0.75)) AS running_age_scaled_quartiles,
    SIZE(COLLECT_SET(segment)) AS num_segments,
    COUNT(*) AS event_count
FROM deduped
GROUP BY segment
HAVING event_count < 1000
ORDER BY smb_event_seq DESC
