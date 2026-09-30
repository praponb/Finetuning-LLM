WITH base AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        CAST(started_at AS DATE) AS started_at_day
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-04-04 20:00:00'
),
latest_match AS (
    SELECT
        s.id,
        s.customer_id,
        s.device,
        s.browser,
        s.started_at,
        x.web_event_event_time
    FROM base s
    JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    SELECT
        s.id,
        s.customer_id,
        s.web_event_event_time,
        c.age,
        c.name,
        c.credit_limit
    FROM latest_match s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
enriched_sessions AS (
    SELECT
        *,
        STDDEV_SAMP(credit_limit) OVER w_grp AS customer_id_stddev_samp_credit_limit,
        AVG(age) OVER (PARTITION BY customer_id ORDER BY age ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS age_avg_5
    FROM enriched
    WINDOW w_grp AS (PARTITION BY customer_id)
),
matched_keys AS (
    SELECT customer_id, id
    FROM enriched_sessions
    EXCEPT
    SELECT customer_id, id
    FROM sessions
    WHERE TO_DATE(started_at) >= '2023-09-01'
),
reconciled AS (
    SELECT s.*
    FROM enriched_sessions s
    JOIN matched_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
eligible AS (
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.session_id IN (15, 16, 36, 37)
        )
)
SELECT
    CASE WHEN age_avg_5 >= 69 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_avg_5_tier,
    CASE WHEN customer_id_stddev_samp_credit_limit >= 15368 THEN 'LARGE' ELSE 'SMALL' END AS customer_id_stddev_samp_credit_limit_tier,
    DATE_TRUNC('YEAR', web_event_event_time) AS year_start,
    MAX(age_avg_5) AS max_age_avg_5,
    MIN(age) AS min_age,
    APPROX_PERCENTILE(age, 0.95) AS age_p95,
    MIN(customer_id_stddev_samp_credit_limit) AS min_customer_id_stddev_samp_credit_limit,
    COUNT(*) AS event_count,
    MAX(web_event_event_time) AS last_web_event_event_time,
    GROUPING_ID() AS grouping_id
FROM eligible
GROUP BY CUBE (CASE WHEN age_avg_5 >= 69 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN customer_id_stddev_samp_credit_limit >= 15368 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('YEAR', web_event_event_time))
HAVING age_p95 >= 0
ORDER BY customer_id_stddev_samp_credit_limit_tier DESC NULLS LAST;
