WITH with_latest AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.session_started_at,
        latest.device
    FROM customers s
    LEFT JOIN LATERAL (
            SELECT c.started_at AS session_started_at, c.device
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.signup_date
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        country,
        is_active,
        age,
        device,
        LEAD(age) OVER w_running AS next_age,
        LEAD(credit_limit) OVER w_running AS next_credit_limit,
        LAG(credit_limit) OVER w_running AS prev_credit_limit,
        ROW_NUMBER() OVER w_running AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age, id) AS row_seq
    FROM with_latest
    WINDOW w_running AS (PARTITION BY country ORDER BY age, id)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_ordered AS event_seq_2,
        AVG(next_age) OVER w_grp AS next_age_avg_5,
        NTILE(5) OVER w_recent AS age_bucket,
        AVG(next_credit_limit) OVER (PARTITION BY country ORDER BY row_seq, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS next_credit_limit_avg_5
    FROM with_windows
    WHERE event_seq >= 4
    WINDOW w_ordered AS (PARTITION BY country ORDER BY age, id), w_grp AS (PARTITION BY country ORDER BY prev_credit_limit, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY device, country ORDER BY age DESC, id)
),
metrics_customers AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        country,
        is_active,
        device,
        next_credit_limit,
        prev_credit_limit,
        event_seq,
        row_seq,
        event_seq_2,
        age_bucket,
        next_credit_limit_avg_5,
        AVG(prev_credit_limit) OVER w_running AS prev_credit_limit_avg_5_prior,
        NTILE(4) OVER (ORDER BY row_seq DESC, id) AS row_seq_bucket
    FROM metrics
    WINDOW w_running AS (ORDER BY age, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN prev_credit_limit >= 16000 THEN 'LARGE' ELSE 'SMALL' END AS prev_credit_limit_tier,
    CASE WHEN next_credit_limit >= 13200 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_credit_limit_tier,
    CASE WHEN age_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_bucket_tier,
    PERCENTILE_APPROX(prev_credit_limit, ARRAY(0.25, 0.5, 0.75)) AS prev_credit_limit_quartiles,
    SUM(CASE WHEN country = 'TH' THEN event_seq ELSE 0 END) AS th_event_seq,
    ROUND(SUM(next_credit_limit) / NULLIF(COUNT(CASE WHEN country = 'US' THEN 1 END), 0), 2) AS next_credit_limit_per_us,
    ROUND(SUM(age_bucket) / NULLIF(COUNT(CASE WHEN country = 'TH' THEN 1 END), 0), 2) AS age_bucket_per_th,
    COUNT_IF(country = 'SG') AS sg_events,
    COUNT(DISTINCT device) AS distinct_devices
FROM metrics_customers
GROUP BY ROLLUP (CASE WHEN prev_credit_limit >= 16000 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN next_credit_limit >= 13200 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN age_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY th_event_seq DESC
