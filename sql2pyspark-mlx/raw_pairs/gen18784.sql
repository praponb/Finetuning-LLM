WITH latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.web_event_event_time
    FROM sessions s
    JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_TRUNC('MONTH', started_at) AS started_at_period
    FROM latest_match
    WHERE browser IS NOT NULL
      AND device = 'mobile'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY browser ORDER BY web_event_event_time NULLS FIRST) AS dedup_rank
        FROM cleaned
    ) t
    WHERE dedup_rank = 1
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.*,
        b.credit_limit,
        b.city,
        b.segment
    FROM ranked e
    LEFT JOIN customers b
        ON e.customer_id = b.id
    WHERE YEAR(b.signup_date) >= 2023
)
-- Aggregate with conditional metrics
SELECT
    device,
    CASE WHEN credit_limit >= 13000 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_tier,
    PERCENTILE_APPROX(credit_limit, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_quartiles,
    SIZE(COLLECT_SET(segment)) AS num_segments,
    COUNT_IF(segment = 'Consumer') AS consumer_events,
    PERCENT_RANK() OVER (PARTITION BY device ORDER BY COUNT_IF(segment = 'Consumer') DESC) AS consumer_events_percent_rank
FROM joined
GROUP BY device, CASE WHEN credit_limit >= 13000 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY num_segments DESC NULLS LAST;
