WITH latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.event_type
    FROM sessions s
    LEFT JOIN LATERAL (
            SELECT c.event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM latest_match
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        event_type,
        SUM(is_new_session) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.started_at,
        a.country,
        d.segment,
        d.signup_date
    FROM sessionized a
    INNER JOIN customers d
        ON a.customer_id = d.id
    WHERE NOT d.is_active
),
gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY segment ORDER BY started_at ASC, id ASC) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY segment ORDER BY started_at ASC, id ASC) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM enriched
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        started_at,
        country,
        segment,
        signup_date,
        SUM(is_new_burst) OVER (PARTITION BY segment ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps_2
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        started_at,
        country,
        segment,
        signup_date,
        AVG(session_no) OVER (PARTITION BY TO_DATE(started_at), customer_id ORDER BY started_at ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS session_no_avg_3,
        DENSE_RANK() OVER (PARTITION BY customer_id, segment ORDER BY session_no DESC) AS session_no_dense_rank,
        SUM(session_no) OVER (PARTITION BY country) AS country_sum_session_no,
        AVG(session_no) OVER (PARTITION BY MONTH(started_at) ORDER BY session_no ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS session_no_avg_6_prior,
        NTILE(5) OVER (PARTITION BY customer_id, segment ORDER BY session_no ASC, id ASC) AS session_no_bucket
    FROM sessions_sessions
)
SELECT
    id,
    started_at,
    country,
    segment,
    signup_date,
    session_no_dense_rank,
    country_sum_session_no,
    session_no_bucket,
    CASE WHEN country RLIKE '^(US|DE)$' THEN 'EXTENDED' WHEN country RLIKE '^(TH|SG)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS country_zone
FROM scored
ORDER BY session_no_dense_rank ASC NULLS FIRST, signup_date ASC;
